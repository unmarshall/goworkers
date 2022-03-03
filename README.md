# Golang Worker Pool

![Build Status](https://github.com/unmarshall/goworkers/actions/workflows/go.yml/badge.svg)
![Go Report Card](https://goreportcard.com/report/github.com/unmarshall/goworkers)

Zero dependency golang goroutines pool library. It is useful in situations where you wish to control the number of goroutines in concurrent activities. It has the following
features:

* Allows consumers to configure maxWorkers and job queue size.
* A pool can also be created with warm workers. This eliminates any small delay in getting a ready worker to process jobs.
* Provides both blocking and non-blocking retrival of results of one or more Jobs
* All jobs are timed and tracked.
* Support for context.Context for every job that is submitted. All deadlines and timeouts should be enforced via context.Context
* APIs provided to create different processor functions - convention is borrowed from functional programming (e.g fn, mapper, consumer). Additions processor functions will be
  introduced in future.
  
**TODO:**
* Add support to configure worker idle timeout and use that to shutdown workers which are no longer actively being used.
* Add support for consumer functional type processor - `consumerFn = func(payload interface{})`
* See if there is a need for a perpetual job which is submitted once to the pool and gets an input channel where it will get the payloads from and then internally it schedules the processing of the payloads as they arrive on available workers. Processor for this will take a channel `channelConsumerFn = func(payloadC <-chan interface{})`. This type of job you can only submit once. One can stop these tasks via cancelling the context. If the result needs to be also pushed to a channel then there always needs to be an active consumer of results from the results channel. Think over it, if its really needed.

## Usage:

To use this library in your golang project, you can get the dependency via:

```bash
go get -u github.com/unmarshall/goworkers
```

### Create a new Pool

```go
func main() {
    // Create a new Pool. Every pool has an id associated to it. There are optional
    // configuration which you configure the pool with. The below configuration will
    // create a pool ("pool-1") with max 10 workers, a job queue which is big enough to hold
    // up to 100 jobs and the pool will be started with 2 live workers waiting for jobs.
    // if optional configuration of `WithWarmWorkers` is not set then pool will not have any live workers
    // and workers will be created/scaled up to Max-Workers when jobs are submitted.
    p, err := gwp.NewPool("pool-1", 10, gwp.WithMaxJobs(100), gwp.WithWarmWorkers(2))
    if err != nil {
        panic(err) // you can handle it differently. This is just an example
    }
    defer func () {
        _ = p.Close() // once you are done with the pool you should close it.
    }()
}
```

### Creating and submitting a simple Job

A new job is created passing a `processor` which takes no input and returns only `error`. If it needs any input for its processing then it can be passed to it via its closure.

```go
func main() {
    // create an initialize pool
    // ------------------------------------------------------------------------------
    // Creating a simple processor function and submitting it.
    jobFuture, err := p.NewJob(context.Background(), "job1", processor).Process()
    // err can happen only if there is an issue submitting this Job. If the job is submitted
    // to the pool then any error thereafter can be accessed via JobFuture
    if err != nil {
        // handle error
    }
    // Await will block till the result is available or the context has been cancelled.
    // There is a non-blocking variant `jobFuture.Stream()` which will provide you a channel
    // on which you can poll for the `JobResult` once it is available
    jobResult := jobFuture.Await()
    // Each JobResult will contain a result (optional, in the above case processor function passed in the job does not return any value other than error)
    // Additional JobResult will contain status, error (if any) and metrics.
    fmt.Printf("result: %v, status: %s, error: %+v, metric: %+v", jobResult.Result, jobResult.Status, jobResult.Err, jobResult.Metric)
}

func processor() error {
// your processing code should go here. 
return nil
}

```

### Create a mapper job

Mapper in functional programming is a function which takes an input and returns an output (of the same or different type).

```go
func main() {
    // create an initialize pool
    // ------------------------------------------------------------------------------
    // create payload that you wish to pass
    var payload interface{}

    // Creating a simple processor function and submitting it.
    jobFuture, err := p.NewMapperJob(context.Background(), "job1", mapProcessor).ProcessPayload(payload)
    // err can happen only if there is an issue submitting this Job. If the job is submitted
    // to the pool then any error thereafter can be accessed via JobFuture
    if err != nil {
    // handle error
    }
    // Await will block till the result is available or the context has been cancelled.
    // There is a non-blocking variant `jobFuture.Stream()` which will provide you a channel
    // on which you can poll for the `JobResult` once it is available
    jobResult := jobFuture.Await()
    // Each JobResult will contain a result (optional, in the above case processor function passed in the job does not return any value other than error)
    // Additional JobResult will contain status, error (if any) and metrics.
    fmt.Printf("result: %v, status: %s, error: %+v, metric: %+v", jobResult.Result, jobResult.Status, jobResult.Err, jobResult.Metric)
}

func mapProcessor(payload interface{}) (interface{}, error) {
	// your processing code should go here. 
	return nil, nil
}
```

### Process batch payloads via Mapper Job

If you have a batch of payloads that you wish to process concurrently then you can submit all the payloads together.
> NOTE: Ensure that there is sufficient capacity that you have configured as `pool`'s jobQ size, else your request will be rejected.

```go

func main() {
    // create an initialize pool
    // ------------------------------------------------------------------------------
    // create payloads that you wish to pass
    var payloads []interface{}

    // Creating a simple processor function and submitting it.
    jobFuture, err := p.NewMapperJob(context.Background(), "job1", mapProcessor).ProcessPayloadBatch(payloads)
    // err can happen only if there is an issue submitting this Job. If the job is submitted
    // to the pool then any error thereafter can be accessed via JobFuture
    if err != nil {
        // handle error
    }
    // jobFuture.Stream will return a channel which will contain one or more JobResults (one per payload).
    resultsChannel := jobFuture.Stream()
    // you can range over the resultsChannel to get the results
    results := make([]JobResult, 0, len(payloads))
    for r := range resultsChannel {
    	results = append(results, r)
    }
}

func mapProcessor(payload interface{}) (interface{}, error) {
// your processing code should go here. 
return nil, nil
}
```
