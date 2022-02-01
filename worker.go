package gwp

import "log"

type worker struct {
	workerJobRequest
	quitC chan struct{}
	pool  *Pool
}

type workerJobRequest struct {
	id   string
	jobC chan Job
}

func newWorker(id string, pool *Pool) *worker {
	quitC := make(chan struct{})
	jobC := make(chan Job, 1)
	return &worker{
		workerJobRequest: workerJobRequest{id: id, jobC: jobC},
		quitC:            quitC,
		pool:             pool,
	}
}

func (w *worker) start() {
	w.pool.incRunningWorker()
	// signal availability as soon as its started, this ensures that the signalling to pool is done immediately instead of waiting for go runtime to schedule the go-routine which
	// runs the for-select loop
	if !w.signalAvailability() {
		return
	}
	go func() {
		defer func() {
			logger.Printf("(start) exiting for-select loop for worker: %s", w.id)
			close(w.jobC)
			w.jobC = nil
			delete(w.pool.workers, w.id)
			w.pool.decRunningWorker()
			log.Printf("Stopped worker %s", w.id)
			w.pool.wg.Done()
		}()
		for {
			select {
			case <-w.quitC:
				logger.Printf("Received a request to close worker: %s. Stopping worker.", w.id)
				w.quitC = nil
				return
			case job, ok := <-w.jobC:
				if !ok {
					// logger.Printf("Job channel for worker:%s has been closed", w.id)
					return
				}
				logger.Printf("Worker: %s received Job: %s", w.id, job.id)
				select {
				case <-job.ctx.Done():
					logger.Printf("Context has been cancelled for job :%s", job.id)
					job.ResultC <- JobResult{
						Result: nil,
						Err:    job.ctx.Err(),
					}
					continue
				default:
					// logger.Printf("worker %s, processing job: %s", w.id, job.id)
					processJob(job)
				}
			}
			if !w.signalAvailability() {
				return
			}
		}
	}()
}

func (w *worker) signalAvailability() bool {
	// Register availability by pushing the jobQ to worker Pool job Queue
	select {
	case w.pool.workerJobQ <- w.workerJobRequest:
		// logger.Printf("worker %s is now available, pushed its jobC to pool", w.id)
		return true
	default:
		// logger.Printf("Looks like the pool's workerJobQ has been closed. Exiting this worker")
		return false
	}
}

func processJob(job Job) {
	defer close(job.ResultC)
	var result JobResult
	if job.isSupplier() {
		result = job.supplier()
	} else {
		result = job.mapper(job.payload)
	}
	job.ResultC <- result
}

func (w *worker) Close() error {
	logger.Printf("Stopping worker %s", w.id)
	close(w.quitC)
	return nil
}
