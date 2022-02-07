package gwp

import (
	"log"
	"time"
)

type worker struct {
	workerTaskRequest
	quitC chan struct{}
	pool  *pool
}

type workerTaskRequest struct {
	id    string
	taskC chan task
}

func newWorker(id string, pool *pool) *worker {
	quitC := make(chan struct{})
	taskC := make(chan task, 1)
	return &worker{
		workerTaskRequest: workerTaskRequest{id: id, taskC: taskC},
		quitC:             quitC,
		pool:              pool,
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
			close(w.taskC)
			w.taskC = nil
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
			case t, ok := <-w.taskC:
				if !ok {
					// logger.Printf("task channel for worker:%s has been closed", w.id)
					return
				}
				logger.Printf("Worker: %s received task: %s", w.id, t.id)
				select {
				case <-t.job.ctx.Done():
					logger.Printf("Context has been cancelled for task :%s", t.id)
					t.resultC <- JobResult{
						Result: nil,
						Status: Cancelled,
						Err:    t.job.ctx.Err(),
					}
					continue
				default:
					logger.Printf("worker %s, processing task: %s", w.id, t.id)
					processJob(t)
				}
			}
			if !w.signalAvailability() {
				return
			}
		}
	}()
}

func (w *worker) signalAvailability() bool {
	// Register availability by pushing the taskQ to worker pool job Queue
	select {
	case w.pool.workerTaskQ <- w.workerTaskRequest:
		logger.Printf("worker %s is now available, pushed its taskC to pool", w.id)
		return true
	default:
		logger.Printf("Looks like the pool's workerTaskQ has been closed. Exiting this worker")
		return false
	}
}

func processJob(t task) {
	defer close(t.resultC)
	t.runStartTime = time.Now()
	var jobResult JobResult
	if t.job.isMapper() {
		result, err := t.job.mapper(t.payload)
		t.runEndTime = time.Now()
		jobResult = createJobResult(result, err, t.getMetric())
	} else {
		err := t.job.processor()
		t.runEndTime = time.Now()
		jobResult = createJobResult(nil, err, t.getMetric())
	}
	t.resultC <- jobResult
	logger.Printf("pushed jobResult: %v to result queue of task: %s", jobResult, t.id)
}

func createJobResult(result interface{}, err error, metric JobMetric) JobResult {
	var status Status
	if err != nil {
		status = Failed
	} else {
		status = Success
	}
	return JobResult{
		Result: result,
		Status: status,
		Err:    err,
		Metric: metric,
	}
}

func (w *worker) Close() error {
	logger.Printf("Stopping worker %s", w.id)
	close(w.quitC)
	return nil
}
