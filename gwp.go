package gwp

import (
	"context"
	"fmt"
	"log"
	"os"
)

var logger = log.New(os.Stderr, "(pool): ", log.LstdFlags|log.Lmicroseconds)

type Any interface{}

// SupplierFn is a supplier of JobResult
type SupplierFn func() JobResult

// MapperFn takes in an input of type Any and returns a JobResult
type MapperFn func(Any) JobResult

// Job is a unit of work that will be executed by an available worker
type Job struct {
	id       string
	supplier SupplierFn
	mapper   MapperFn
	payload  Any
	ctx      context.Context
	ResultC  chan JobResult
}

// JobResult encapsulates both a result of type Any and optionally error
type JobResult struct {
	Result Any
	Err    error
}

func (j Job) isSupplier() bool {
	return j.supplier != nil
}

// NewSupplierJob constructs a new Job with SupplierFn
func NewSupplierJob(ctx context.Context, id string, supplier SupplierFn) Job {
	resultC := make(chan JobResult)
	return Job{
		id:       id,
		supplier: supplier,
		ctx:      ctx,
		ResultC:  resultC,
	}
}

// NewMapperJob constructs a new Job with MapperFn and a single payload of type Any
func NewMapperJob(ctx context.Context, id string, mapper MapperFn, payload Any) Job {
	resultC := make(chan JobResult)
	return Job{
		id:      id,
		mapper:  mapper,
		payload: payload,
		ctx:     ctx,
		ResultC: resultC,
	}
}

// NewMapperBatchJob is a convenience constructor for creating batch mapper jobs
func NewMapperBatchJob(ctx context.Context, id string, mapper MapperFn, payloads []Any) []Job {
	jobs := make([]Job, 0, len(payloads))
	for i, p := range payloads {
		jobID := fmt.Sprintf("%s-%d", id, i)
		jobs = append(jobs, NewMapperJob(ctx, jobID, mapper, p))
	}
	return jobs
}
