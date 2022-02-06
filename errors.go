package gwp

import (
	"errors"
	"fmt"
	"strings"
)

//go:generate stringer -type=ErrorCode
type ErrorCode int

const (
	_ ErrorCode = iota
	PoolClosed
	InvalidMaxWorker
	InvalidWarmWorkers
	PoolJobQueueFull
	InsufficientCapacityForBatchJob
)

var (
	PoolClosedErr = Wrapf(nil, PoolClosed, "pool is closed. No further job requests can be entertained")
)

type WorkerPoolError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func Wrapf(cause error, code ErrorCode, format string, args ...interface{}) WorkerPoolError {
	return WorkerPoolError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		Cause:   cause,
	}
}

func (e WorkerPoolError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("(%d) %s %+v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("(%d) %s", e.Code, e.Message)
}

type MultiWorkerPoolError struct {
	errors errorChain
}

func NewMultiWorkerPoolError() *MultiWorkerPoolError {
	return &MultiWorkerPoolError{}
}

func (me MultiWorkerPoolError) Error() string {
	if len(me.errors) == 1 {
		return fmt.Sprintf("%+v", me.errors[0])
	}
	errReport := make([]string, 0, len(me.errors)+1)
	errReport = append(errReport, fmt.Sprintf("%d errors occurred", len(me.errors)))
	for _, err := range me.errors {
		errReport = append(errReport, err.Error())
	}
	return strings.Join(errReport, "\n")
}

func (me *MultiWorkerPoolError) AppendError(err error) {
	me.errors = append(me.errors, err)
}

func (me *MultiWorkerPoolError) WrappedErrors() []error {
	if me == nil {
		return nil
	}
	return me.errors
}

func (me *MultiWorkerPoolError) Unwrap() error {
	// If we have no errors then we do nothing
	if me == nil || len(me.errors) == 0 {
		return nil
	}
	// If we have exactly one error, we can just return that directly.
	if len(me.errors) == 1 {
		return me.errors[0]
	}
	// Shallow copy the slice
	errs := make([]error, len(me.errors))
	copy(errs, me.errors)
	return errorChain(errs)
}

type errorChain []error

func (e errorChain) Error() string {
	if len(e) == 0 {
		return ""
	}
	return e[0].Error()
}

// Unwrap implements errors.Unwrap by returning the next error in the
// chain or nil if there are no more errors.
func (e errorChain) Unwrap() error {
	if len(e) == 1 {
		return nil
	}
	return e[1:]
}

// As implements errors.As by attempting to map to the current value.
func (e errorChain) As(target interface{}) bool {
	if len(e) == 0 {
		return false
	}
	return errors.As(e[0], target)
}

// Is implements errors.Is by comparing the current value directly.
func (e errorChain) Is(target error) bool {
	if len(e) == 0 {
		return false
	}
	return errors.Is(e[0], target)
}
