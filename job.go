package gwp

import (
	"context"
	"fmt"
)

// job is a unit of work that will be executed by an available worker
type job struct {
	id        string
	processor Fn
	mapper    MapperFn
	ctx       context.Context
	status    Status
	pool      *pool // holds a reference to the pool to which it will be submitted
}

// task is a unit of work that will be processed by a worker.
// A job can consist of one or more tasks. Calling any Process* methods
// on a Job will result in creation or one or more tasks.
type task struct {
	id      string
	job     *job
	resultC chan JobResult
	payload interface{}
}

func (j *job) newTask(payload interface{}, taskNum int) task {
	taskResultC := make(chan JobResult, 1)
	return task{
		id:      fmt.Sprintf("%s-%d", j.id, taskNum),
		job:     j,
		resultC: taskResultC,
		payload: payload,
	}
}

func (j job) isMapper() bool {
	return j.mapper != nil
}

func (j job) GetID() string {
	return j.id
}

func (j job) GetStatus() Status {
	return j.status
}

func (j *job) Process() (JobFuture, error) {
	t := j.newTask(nil, 0)
	return j.submitTask(t)
}

func (j *job) ProcessPayload(payload interface{}) (JobFuture, error) {
	if !j.isMapper() {
		return nil, fmt.Errorf("job %s has a function defined which does not take any payload. You should call Process instead", j.id)
	}
	t := j.newTask(payload, 0)
	return j.submitTask(t)
}

func (j *job) ProcessPayloadBatch(payloads []interface{}) (JobFuture, error) {
	if !j.isMapper() {
		return nil, fmt.Errorf("job %s has a function defined which does not take any payload. You should call Process instead", j.id)
	}
	numPayloads := len(payloads)
	if accommodate, availableCap := j.pool.canAccommodateTasksAndGetAvailableCap(numPayloads); !accommodate {
		return nil, Wrapf(nil, InsufficientCapacityForBatchJob, "Requested processing of %d payloads, available capacity: %d", numPayloads, availableCap)
	}
	// create and submit tasks - one per payload
	tasks := make([]task, 0, numPayloads)
	for i, p := range payloads {
		t := j.newTask(p, i)
		tasks = append(tasks, t)
	}
	return j.submitBatch(tasks)
}

func (t *task) pushErrorResult(status Status, err error) {
	t.resultC <- JobResult{
		Result: nil,
		Status: status,
		Err:    err,
	}
}

func (j *job) submitTask(t task) (JobFuture, error) {
	err := j.doSubmitTask(t)
	if err != nil {
		j.status = Failed
		return nil, err
	}
	return jobFuture{job: j, taskResultChannels: []<-chan JobResult{t.resultC}}, nil
}

func (j *job) doSubmitTask(t task) error {
	err := j.pool.doSubmit(t)
	if err != nil {
		close(t.resultC)
		return err
	}
	return nil
}

func (j *job) submitBatch(tasks []task) (JobFuture, error) {
	numTasks := len(tasks)
	taskResultChannels := make([]<-chan JobResult, 0, numTasks)
	submissionErrors := NewMultiWorkerPoolError()
	errCount := 0
	for _, t := range tasks {
		err := j.doSubmitTask(t)
		if err != nil {
			errCount += 1
			submissionErrors.AppendError(err)
		} else {
			taskResultChannels = append(taskResultChannels, t.resultC)
		}
	}
	if errCount > 0 {
		if errCount == numTasks {
			j.status = Failed
			return nil, submissionErrors
		} else {
			return jobFuture{job: j, taskResultChannels: taskResultChannels}, submissionErrors
		}
	}
	return jobFuture{job: j, taskResultChannels: taskResultChannels}, nil
}
