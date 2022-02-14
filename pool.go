package gwp

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxJobs int = 1000
)

type pool struct {
	id          string
	maxWorkers  int
	maxJobs     int
	running     int32
	workers     map[string]io.Closer
	taskQ       chan task
	workerTaskQ chan workerTaskRequest
	quitC       chan struct{}
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func (p *pool) NewJob(ctx context.Context, id string, fn Fn) Job {
	return &job{
		id:        id,
		processor: fn,
		ctx:       ctx,
		pool:      p,
	}
}

func (p *pool) NewMapperJob(ctx context.Context, id string, mapperFn MapperFn) Job {
	return &job{
		id:     id,
		mapper: mapperFn,
		ctx:    ctx,
		pool:   p,
	}
}

func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	logger.Printf("Closing pool %s\n", p.id)
	go func() {
		logger.Printf("Attempting to stop workers for pool: %s", p.id)
		for _, w := range p.workers {
			_ = w.Close()
		}
	}()
	p.wg.Wait()
	close(p.quitC)
	logger.Printf("Closed pool %s", p.id)
	return nil
}

func (p *pool) warmUpWorkers(n int) {
	for i := 0; i < n; i++ {
		p.createAndRegisterNewWorker()
	}
}

func (p *pool) start() {
	defer func() {
		close(p.taskQ)
		close(p.workerTaskQ)
		p.taskQ = nil
		p.workerTaskQ = nil
		logger.Printf("pool: %s has been shutdown", p.id)
	}()
	for {
		select {
		case <-p.quitC:
			logger.Printf("(start) Received request to shutdown pool: %s", p.id)
			return
		case t, ok := <-p.taskQ:
			if !ok {
				logger.Printf("(start) pool taskQ has been closed.")
				return
			}
			logger.Printf("(start) Attempting to get worker task channel for task: %s", t.id)
			wtaskC, status, err := p.getWorkerTaskChannel(t)
			if err != nil {
				// error can only happen if either the pool has been closed
				// or job for which this task has been created has been cancelled via its context
				t.resultC <- JobResult{Result: nil, Status: status, Err: err}
				close(t.resultC)
			}
			select {
			case wtaskC <- t:
			default:
				logger.Printf("(start) looks like worker job channel is closed. Cannot process task : %s", t.id)
			}
		default:
		}
	}
}

func (p *pool) getWorkerTaskChannel(t task) (chan<- task, Status, error) {
	wTaskC, status, err := p.getAvailableWorkerTaskChannel(t)
	if err != nil {
		return nil, status, err
	}
	// If there is an available worker job channel then immediately return it
	if wTaskC != nil {
		return wTaskC, status, nil
	}
	// If no worker job channel is available then check if we can create new workers
	p.mu.Lock()
	if startedWorkers := len(p.workers); startedWorkers < p.maxWorkers {
		logger.Printf("Currently there is no available worker to process task: %s. Creating a new worker for pool: %s", t.id, p.id)
		p.createAndRegisterNewWorker()
	}
	p.mu.Unlock()
	return p.waitForAvailableWorker(t)
}

func (p *pool) getAvailableWorkerTaskChannel(t task) (chan<- task, Status, error) {
	select {
	case <-t.job.ctx.Done():
		logger.Printf("Context for job: %s has been cancelled", t.job.id)
		return nil, Cancelled, t.job.ctx.Err()
	case <-p.quitC:
		return nil, Failed, PoolClosedErr
	case wTaskRequest, ok := <-p.workerTaskQ:
		if !ok {
			return nil, Failed, PoolClosedErr
		}
		logger.Printf("Got an available worker task channel :%s to process task: %s", wTaskRequest.id, t.id)
		return wTaskRequest.taskC, Processing, nil
	default:
		return nil, Enqueued, nil
	}
}

func (p *pool) waitForAvailableWorker(t task) (chan<- task, Status, error) {
	for {
		wtaskC, status, err := p.getAvailableWorkerTaskChannel(t)
		if err != nil {
			return nil, status, err
		}
		if wtaskC != nil {
			return wtaskC, status, nil
		}
	}
}

func (p *pool) createAndRegisterNewWorker() {
	wID := fmt.Sprintf("%s_Worker-%d", p.id, len(p.workers))
	w := newWorker(wID, p)
	p.workers[wID] = w
	w.start()
	p.wg.Add(1)
	logger.Printf("(createAndRegisterNewWorker) created and started a new worker: %s for pool: %s. Total running workers: %d\n", w.id, p.id, len(p.workers))
}

func (p *pool) incRunningWorker() {
	atomic.AddInt32(&p.running, 1)
}

func (p *pool) decRunningWorker() {
	atomic.AddInt32(&p.running, -1)
}

func (p *pool) canAccommodateTasksAndGetAvailableCap(taskCount int) (bool, int) {
	if p.maxJobs == -1 {
		return true, -1
	}
	availableCap := p.maxJobs - len(p.taskQ)
	return availableCap >= taskCount, availableCap
}

func (p *pool) submit(t task) error {
	if len(p.taskQ) == p.maxJobs {
		return Wrapf(nil, PoolJobQueueFull, "pool job Queue is full with capacity: %d, Cannot accept task: %s till workers pick up tasks from the queue", len(p.taskQ), t.id)
	}
	t.enqueueTime = time.Now()
	select {
	case <-p.quitC:
		return PoolClosedErr
	case <-t.job.ctx.Done():
		return t.job.ctx.Err()
	case p.taskQ <- t:
		return nil
	}
}
