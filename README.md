# Golang Worker Pool

Zero dependency golang goroutines pool library. It is useful in situations where you wish to control the number of goroutines in concurrent activities. It has the following
features:

* Allows consumers to configure maxWorkers and job queue size.
* A pool can also be created with warm workers. This eliminates any small delay in getting a ready worker to process jobs.
* Provides both blocking and non-blocking retrival of results of one or more Jobs
* All jobs are timed and tracked.
* Support for context.Context for every job that is submitted. All deadlines and timeouts should be enforced via context.Context
* APIs provided to create different processor functions - convention is borrowed from functional programming (e.g fn, mapper, consumer). Additions processor functions will be
  introduced in future.

## Usage:

To use this library in your golang project, you can get the dependency via:

```bash
go get -u github.com/unmarshall/goworkers
```

```go
package main

import (
  "context"
  "fmt"

  gwp "github.com/unmarshall/goworkers"
)

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
  defer func() {
    _ = p.Close() // once you are done with the pool you should close it.
  }()

  // Creating a simple processor function and submitting it.
  jobFuture, err := p.NewJob(context.Background(), "job1", func() error {
    // your processing code should go here. 
    return nil
  }).Process()
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
  fmt.Printf("result: %v, status: %s, error: %+v, metric: %+v", jobResult.Result,  jobResult.Status, jobResult.Err, jobResult.Metric)

}

```
