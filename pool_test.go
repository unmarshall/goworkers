package gwp

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	expectedPoolId := "pool-test-newpool"
	p, err := NewPool(expectedPoolId, 10, WithMaxJobs(100))
	defer func(p *Pool) {
		_ = p.Close()
	}(p)
	if err != nil {
		t.Fatalf("Error creating pool.")
	}
	if p.id != expectedPoolId {
		t.Errorf("Expected pool expectedPoolId: %s, actual: %s", expectedPoolId, p.id)
	}
	if p.maxWorkers != 10 {
		t.Errorf("Expected pool maxWorkers = 10 instead found: %d", p.maxWorkers)
	}
	if p.running != 0 {
		t.Errorf("Expected 0 running workers, instead found %d", p.running)
	}
}

func TestNewPoolWithInvalidMaxWorkers(t *testing.T) {
	p, err := NewPool("pool-0", -2, WithMaxJobs(100))
	if err == nil {
		t.Errorf("Expected an error but instead there is no error returned")
	}
	if p != nil {
		t.Errorf("Expected nil pool found %v", p)
	}
}

func TestNewPoolWithInvalidWarmedUpWorkers(t *testing.T) {
	expectedPoolId := "pool-test-new-invalid-warmed-up-pool"
	p, err := NewPool(expectedPoolId, 10, WithMaxJobs(100), WithWarmWorkers(20))
	if err == nil {
		t.Errorf("Expected an error since the warmed up workers are greater than max workers, found none")
	}
	if p != nil {
		t.Errorf("Expected nil pool found %v", p)
	}
}

func TestNewPoolWithValidWarmedUpWorkers(t *testing.T) {
	expectedPoolId := "pool-test-new-warmed-up-pool"
	const warmedUpWorkers int = 5
	p, err := NewPool(expectedPoolId, 10, WithMaxJobs(100), WithWarmWorkers(warmedUpWorkers))
	defer func(p *Pool) {
		_ = p.Close()
	}(p)
	if err != nil {
		t.Fatalf("Error creating pool.")
	}
	if len(p.workers) != warmedUpWorkers {
		t.Errorf("Expected %d warmed up workers instead found %d", warmedUpWorkers, len(p.workers))
	}
	if p.running != int32(warmedUpWorkers) {
		t.Errorf("Expected %d running workers, instead found %d", warmedUpWorkers, p.running)
	}
}

func TestClosePool(t *testing.T) {
	p, err := NewPool("pool-test-close-pool", 10, WithMaxJobs(100))
	if err != nil {
		t.Fatalf("Error creating pool")
	}
	if len(p.workers) != 0 {
		t.Errorf("Expected no running workers, instead found %d", len(p.workers))
	}
	ctx := context.Background()
	job := NewSupplierJob(ctx, "j1", createSupplierFn(ctx, 200))
	p.Execute(job)
	if p.running != 1 {
		t.Errorf("Expected running workers 1 instead found :%d", p.running)
	}
	err = p.Close()
	if err != nil {
		t.Fatalf("Unexpected error closing pool %v", err)
	}
	if len(p.workers) != 0 {
		t.Errorf("Expected no running workers, instead found: %d", len(p.workers))
	}
}

func TestExecute(t *testing.T) {
	p, err := NewPool("pool-test-submit", 2, WithMaxJobs(10))
	if err != nil {
		t.Fatalf("Error creating pool")
	}
	defer func() {
		_ = p.Close()
	}()
	jobResults := make([]JobResult, 0, 10)
	for i := 0; i < 10; i++ {
		ctx := context.Background()
		jobID := "j" + strconv.Itoa(i)
		jobR := p.Execute(NewSupplierJob(ctx, jobID, createSupplierFn(ctx, 10)))
		logger.Printf("Job: %s, Result: %v", jobID, jobR)
		jobResults = append(jobResults, jobR)
	}
	if len(jobResults) != 10 {
		t.Errorf("Expected 10 job results but found: %d", len(jobResults))
	}
}

func TestExecuteWithTimeout(t *testing.T) {
	p, err := NewPool("pool-test-submit", 2, WithMaxJobs(10))
	if err != nil {
		t.Fatalf("Error creating pool")
	}
	defer func() {
		_ = p.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Millisecond)
	defer cancel()
	result := p.Execute(NewSupplierJob(ctx, "jobWithTimeout", createSupplierFn(ctx, 50)))
	if result.Err == nil {
		t.Errorf("Expected a cancellation error to be returned, found none")
	}
	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Errorf("Expected %v error, found %v", context.DeadlineExceeded, reflect.TypeOf(result.Err))
	}
}

func TestExceedingPoolJobQueue(t *testing.T) {
	p, err := NewPool("pool-test-exceed-jobQ", 2, WithMaxJobs(5))
	if err != nil {
		t.Fatalf("Error creating pool")
	}
	const totalJobs int = 30
	var jobErr error
	var jobResults []JobResult
	defer func() {
		if jobErr == nil {
			t.Errorf("Exepected an error found none")
		}
		_ = p.Close()
	}()
	jobResultChannels := make([]<-chan JobResult, 0, 100)

L:
	for i := 0; i < totalJobs; i++ {
		ctx := context.Background()
		job := NewSupplierJob(ctx, "j"+strconv.Itoa(i), createSupplierFn(ctx, 500))
		jobErr = p.TrySubmit(job)
		if jobErr != nil {
			break L
		}
		jobResultChannels = append(jobResultChannels, job.ResultC)
	}
	jobResults = collectJobResults(jobResultChannels)
	if len(jobResults) == totalJobs {
		t.Errorf("Expected lesser than %d jobs to be processed instead all jobs got processed", totalJobs)
	}
}

func TestSubmitMapperBatchJobs(t *testing.T) {
	p, err := NewPool("pool-test-exceed-jobQ", 5, WithMaxJobs(100))
	if err != nil {
		t.Fatalf("(TestSubmitMapperBatchJobs) Error creating pool")
	}
	defer func() {
		_ = p.Close()
	}()
	const totalJobs int = 30
	ctx := context.Background()
	payloads := make([]Any, 0, totalJobs)
	for i := 0; i < totalJobs; i++ {
		payloads = append(payloads, rand.Intn(30))
	}
	submittedJobs, resultsC, err := p.SubmitMapperBatchJobs(ctx, "mapBatchTest", createMapperFn(ctx, 100), payloads)
	if err != nil {
		t.Errorf("(TestSubmitMapperBatchJobs) Unexpected error %v\n", err)
	}
	if submittedJobs != totalJobs {
		t.Errorf("(TestSubmitMapperBatchJobs) Expected successful submission of %d jobs, actual: %d", totalJobs, submittedJobs)
	}
	jobResults := make([]JobResult, 0, totalJobs)
	var erroneousResults int
	for r := range resultsC {
		jobResults = append(jobResults, r)
		if r.Err != nil {
			erroneousResults += 1
		}
	}
	if erroneousResults > 0 {
		t.Errorf("Expected no errors, instead found %d errorneous results", erroneousResults)
	}
	if len(jobResults) != totalJobs {
		t.Errorf("Expected %d results, actual: %d", totalJobs, len(jobResults))
	}
}

func collectJobResults(resultChannels []<-chan JobResult) []JobResult {
	jobResults := make([]JobResult, 0, len(resultChannels))
	var wg sync.WaitGroup
	multiplex := func(c <-chan JobResult) {
		defer wg.Done()
		for r := range c {
			jobResults = append(jobResults, r)
		}
	}
	wg.Add(len(resultChannels))
	for _, c := range resultChannels {
		go multiplex(c)
	}
	wg.Wait()
	return jobResults
}

func createMapperFn(ctx context.Context, sleep int) func(Any) JobResult {
	return func(payload Any) JobResult {
		after := time.After(time.Duration(sleep) * time.Millisecond)
		tick := time.Tick(time.Duration(1) * time.Millisecond)
		i, ok := payload.(int)
		if !ok {
			return JobResult{Result: nil, Err: fmt.Errorf("failed to convert payload %v to int", payload)}
		}
		var counter int
	L:
		for {
			select {
			case <-ctx.Done():
				logger.Printf("mapper job cancelled due to timeout")
				return JobResult{nil, ctx.Err()}
			case <-after:
				break L
			case <-tick:
				counter += 1
			}
		}
		return JobResult{
			Result: i * counter,
			Err:    nil,
		}
	}
}

func createSupplierFn(ctx context.Context, sleep int) func() JobResult {
	return func() JobResult {
		var counter int
		after := time.After(time.Duration(sleep) * time.Millisecond)
		tick := time.Tick(time.Duration(1) * time.Millisecond)
	L:
		for {
			select {
			case <-ctx.Done():
				logger.Printf("supplier job cancelled due to timeout")
				return JobResult{nil, ctx.Err()}
			case <-after:
				break L
			case <-tick:
				counter += 1
			}
		}
		return JobResult{
			Result: counter,
			Err:    nil,
		}
	}
}
