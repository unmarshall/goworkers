package gwp

import (
	"context"
	"sync"
)

type jobFuture struct {
	job                *job
	taskResultChannels []<-chan JobResult
}

func (jf jobFuture) Await() JobResult {
	taskResultC := jf.taskResultChannels[0]
	for {
		select {
		case <-jf.job.ctx.Done():
			logger.Printf("(jobfuture) Job has been cancelled")
			return JobResult{
				Status: Cancelled,
				Err:    jf.job.ctx.Err(),
			}
		case <-jf.job.pool.quitC:
			logger.Printf("(jobFuture) pool has been close")
			return JobResult{
				Status: Failed,
				Err:    PoolClosedErr,
			}
		case result := <-taskResultC:
			return result
		}
	}
}

func (jf jobFuture) AwaitAll() []JobResult {
	results := make([]JobResult, 0, len(jf.taskResultChannels))
	resultC := fanInJobResults(jf.job.ctx, jf.taskResultChannels...)
	for result := range resultC {
		results = append(results, result)
	}
	return results
}

func (jf jobFuture) Stream() <-chan JobResult {
	return fanInJobResults(jf.job.ctx, jf.taskResultChannels...)
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