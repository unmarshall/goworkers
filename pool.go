package gwp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	merr "github.tools.sap/IO-CLOUD/multi-error"
)

const (
	defaultMaxJobs int = 1000
)

var (
	PoolClosedErr = errors.New("pool is closed. No further job requests can be entertained")
)

type Pool struct {
	id         string
	maxWorkers int
	maxJobs    int
	running    int32
	workers    map[string]io.Closer
	jobQ       chan Job
	workerJobQ chan workerJobRequest
	quitC      chan struct{}
	mu         sync.Mutex
	wg         sync.WaitGroup
}

func NewPool(id string, maxWorkers int, options ...PoolOption) (*Pool, error) {
	if maxWorkers <= 0 {
		return nil, fmt.Errorf("maxWorkers should be greater than 0. Value passed: %d\n", maxWorkers)
	}
	opts := buildPoolOptions(options...)
	maxJobs := opts.MaxJobs
	if maxJobs <= 0 {
		maxJobs = defaultMaxJobs
	}
	if opts.WarmWorkers > maxWorkers {
		return nil, fmt.Errorf("warm workers: %d cannot be more than max workers: %d", opts.WarmWorkers, maxWorkers)
	}

	quitC := make(chan struct{})
	workers := make(map[string]io.Closer, maxWorkers)
	workerJobQC := make(chan workerJobRequest, maxWorkers)
	jobC := make(chan Job, maxJobs)

	pool := &Pool{
		id:         id,
		maxWorkers: maxWorkers,
		maxJobs:    maxJobs,
		running:    0,
		workers:    workers,
		workerJobQ: workerJobQC,
		jobQ:       jobC,
		quitC:      quitC,
	}
	pool.warmUpWorkers(opts.WarmWorkers)
	go pool.start()
	return pool, nil
}

func (p *Pool) warmUpWorkers(n int) {
	for i := 0; i < n; i++ {
		p.createAndRegisterNewWorker()
	}
}

func (p *Pool) start() {
	defer func() {
		close(p.jobQ)
		close(p.workerJobQ)
		p.jobQ = nil
		p.workerJobQ = nil
		logger.Printf("Pool: %s has been shutdown", p.id)
	}()
	for {
		select {
		case <-p.quitC:
			logger.Printf("Received request to shutdown pool: %s", p.id)
			return
		case job, ok := <-p.jobQ:
			if !ok {
				logger.Printf("(start) pool jobQ has been closed.")
				return
			}
			logger.Printf("(start) Attempting to get worker job channel for job: %s", job.id)
			wJobC, err := p.getWorkerJobChannel(job)
			if err != nil {
				job.ResultC <- JobResult{nil, err}
			}
			select {
			case wJobC <- job:
			default:
				logger.Printf("(start) looks like worker job channel is closed. Cannot process job : %s", job.id)
			}
		default:
		}
	}
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	logger.Printf("Closing pool %s\n", p.id)
	// atomic.CompareAndSwapInt32(&p.status, OPEN, CLOSE)
	close(p.quitC)
	logger.Printf("Sent request to pool's: %s quitC", p.id)
	go func() {
		logger.Printf("Attempting to stop workers for pool: %s", p.id)
		for _, w := range p.workers {
			_ = w.Close()
		}
	}()
	p.wg.Wait()
	logger.Printf("Closed pool %s", p.id)
	return nil
}

func (p *Pool) getWorkerJobChannel(job Job) (chan<- Job, error) {
	wJobC, err := p.getAvailableWorkerJobChannel(job)
	if err != nil {
		return nil, err
	}
	// If there is an available worker job channel then immediately return it
	if wJobC != nil {
		return wJobC, nil
	}
	// If no worker job channel is available then check if we can create new workers
	p.mu.Lock()
	if startedWorkers := len(p.workers); startedWorkers < p.maxWorkers {
		logger.Printf("Currently there is no available worker to process Job: %s. Creating a new worker for pool: %s", job.id, p.id)
		p.createAndRegisterNewWorker()
	}
	p.mu.Unlock()
	return p.waitForAvailableWorker(job)
}

func (p *Pool) getAvailableWorkerJobChannel(job Job) (chan<- Job, error) {
	select {
	case <-job.ctx.Done():
		logger.Printf("Context for job: %s has been cancelled", job.id)
		return nil, job.ctx.Err()
	case <-p.quitC:
		return nil, PoolClosedErr
	case wJobRequest, ok := <-p.workerJobQ:
		if !ok {
			return nil, PoolClosedErr
		}
		logger.Printf("Got an available worker job channel :%s to process job: %s", wJobRequest.id, job.id)
		return wJobRequest.jobC, nil
	default:
		return nil, nil
	}
}

func (p *Pool) waitForAvailableWorker(job Job) (chan<- Job, error) {
	for {
		wJobC, err := p.getAvailableWorkerJobChannel(job)
		if err != nil {
			return nil, err
		}
		if wJobC != nil {
			return wJobC, nil
		}
	}
}

func (p *Pool) createAndRegisterNewWorker() {
	wID := fmt.Sprintf("%s_Worker-%d", p.id, len(p.workers))
	w := newWorker(wID, p)
	p.workers[wID] = w
	w.start()
	p.wg.Add(1)
	logger.Printf("(createAndRegisterNewWorker) created and started a new worker: %s for pool: %s. Total running workers: %d\n", w.id, p.id, len(p.workers))
}

func (p *Pool) incRunningWorker() {
	atomic.AddInt32(&p.running, 1)
}

func (p *Pool) decRunningWorker() {
	atomic.AddInt32(&p.running, -1)
}

func (p *Pool) Execute(job Job) JobResult {
	err := p.doSubmit(job)
	if err != nil {
		return JobResult{nil, err}
	}
	return <-job.ResultC
}

func (p *Pool) TrySubmit(job Job) error {
	logger.Printf("Received a request to process Job: %s", job.id)
	return p.doSubmit(job)
}

func (p *Pool) SubmitMapperBatchJobs(ctx context.Context, id string, mapper MapperFn, payloads []Any) (int, <-chan JobResult, error) {
	// Check if there is sufficient space in the job queue to accommodate this request
	availableCap := p.maxJobs - len(p.jobQ)
	if availableCap < len(payloads) {
		return 0, nil, fmt.Errorf("requested batch size: %d, available: %d", len(payloads), availableCap)
	}
	// create jobs
	jobs := make([]Job, 0, len(payloads))
	jobResultChannels := make([]<-chan JobResult, 0, len(payloads))
	for i, p := range payloads {
		jobID := fmt.Sprintf("%s-%d", id, i)
		job := NewMapperJob(ctx, jobID, mapper, p)
		jobs = append(jobs, job)
		jobResultChannels = append(jobResultChannels, job.ResultC)
	}

	// TrySubmit jobs
	jobsSubmitted := 0
	submissionErrors := merr.NewMultiError()
	for _, j := range jobs {
		err := p.TrySubmit(j)
		if err != nil {
			submissionErrors.AppendError(err)
			continue
		}
		jobsSubmitted += 1
	}
	errorsEncountered := len(submissionErrors.WrappedErrors())
	if errorsEncountered == len(jobs) {
		return jobsSubmitted, nil, submissionErrors
	}
	// FanIn all job result channels
	resultsChannel := fanInJobResults(ctx, jobResultChannels...)
	if errorsEncountered == 0 {
		return jobsSubmitted, resultsChannel, nil
	}
	return jobsSubmitted, resultsChannel, submissionErrors
}

func fanInJobResults(ctx context.Context, channels ...<-chan JobResult) <-chan JobResult {
	var wg sync.WaitGroup
	multiplexStream := make(chan JobResult)
	multiplex := func(c <-chan JobResult) {
		defer wg.Done()
		for result := range c {
			select {
			case <-ctx.Done():
				logger.Printf("(fanInJobResults)(multiplex) Context has been cancelled")
				return
			case multiplexStream <- result:
			}
		}
	}
	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}
	go func() {
		wg.Wait()
		close(multiplexStream)
	}()
	return multiplexStream
}

func (p *Pool) doSubmit(job Job) error {
	if len(p.jobQ) == p.maxJobs {
		return fmt.Errorf("pool Job Queue is full with capacity: %d, Cannot accept job: %s till workers pick up jobs from the queue", len(p.jobQ), job.id)
	}
	select {
	case <-p.quitC:
		return PoolClosedErr
	case p.jobQ <- job:
		return nil
	}
}
