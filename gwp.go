package gwp

import (
	"context"
	"io"
	"log"
	"os"
	"time"
)

var logger = log.New(os.Stderr, "(pool): ", log.LstdFlags|log.Lmicroseconds)

//go:generate stringer -type=Status
type Status int

const (
	Enqueued Status = iota
	Processing
	Success
	Cancelled
	Failed
	Rejected
)

type Any interface{}

// At present there is support to provide two kinds of functions as described below. In general functional programming style you have mappers, consumers, bi-functions etc.
// For now the support has been kept to a minimum of two type of functions. If there is a need we can easily add more function types.
// ----------------------------------------------------------------------------------------------------------------------------------------------

// Fn is function which does not take any input and returns an error if there is error during processing.
// Implementers of the processor can choose to not return any error if they wish to ignore errors.
// All the input to this function (if any) should be captured as a closure.
type Fn func() error

// MapperFn takes in an input of type Any and returns an output of type Any if the mapping was successful else it returns an error.
// This allows for submitting batch jobs where the user needs to pass a common
// processor function and pass a slice of payloads. For each payload a new job will be generated internally and concurrently executed by available workers.
// NOTE: When 1.18 brings in generics then we can define a MapperFn which takes in a T and returns a R and error
type MapperFn func(Any) (Any, error)

// JobMetric captures limited metrics for each Job
type JobMetric struct {
	// runDuration captures the time it took for the processor to complete processing
	runDuration time.Duration
	// enqueueDuration captures the delay in picking up this job from the work queue by an available worker. This metrics can be used to optimize the pool options
	enqueueDuration time.Duration
}

type Pool interface {
	NewJob(ctx context.Context, id string, fn Fn) Job
	NewMapperJob(ctx context.Context, id string, mapperFn MapperFn) Job
	Close() error
}

func NewPool(id string, maxWorkers int, options ...PoolOption) (*pool, error) {
	if maxWorkers <= 0 {
		return nil, Wrapf(nil, InvalidMaxWorker, "maxWorkers should be greater than 0. Value passed: %d\n", maxWorkers)
	}
	opts := buildPoolOptions(options...)
	maxJobs := opts.MaxJobs
	if maxJobs <= 0 {
		maxJobs = defaultMaxJobs
	}
	if opts.WarmWorkers > maxWorkers {
		return nil, Wrapf(nil, InvalidWarmWorkers, "warm workers: %d cannot be more than max workers: %d", opts.WarmWorkers, maxWorkers)
	}

	quitC := make(chan struct{})
	workers := make(map[string]io.Closer, maxWorkers)
	workerJobQC := make(chan workerTaskRequest, maxWorkers)
	jobC := make(chan task, maxJobs)

	pool := &pool{
		id:          id,
		maxWorkers:  maxWorkers,
		maxJobs:     maxJobs,
		running:     0,
		workers:     workers,
		workerTaskQ: workerJobQC,
		taskQ:       jobC,
		quitC:       quitC,
	}
	pool.warmUpWorkers(opts.WarmWorkers)
	go pool.start()
	return pool, nil
}

// Job provides a handle to the user to submit work to the worker pool
type Job interface {
	// GetID returns the identifier with which a Job was created. It is encouraged that each Job gets a unique identifier
	GetID() string
	GetStatus() Status
	Process() (JobFuture, error)
	ProcessPayload(payload Any) (JobFuture, error)
	ProcessPayloadBatch(payloads []Any) (JobFuture, error)
}

type JobResult struct {
	Result Any
	Status Status
	Err    error
	Metric JobMetric
}

type JobFuture interface {
	Await() JobResult
	AwaitAll() []JobResult
	Stream() <-chan JobResult
}
