package ops

import (
	"fmt"

	"github.com/m-lab/etl-gardener/tracker"
)

// Outcome is a custom error for use in this package.
// It is a little unusual in that it can encode a successful outcome,
// in which case Unwrap will return nil.
type Outcome struct {
	job    tracker.Job
	error  // possibly nil
	retry  bool
	detail string
}

// ShouldRetry indicates of the operation should be retried later.
func (o Outcome) ShouldRetry() bool {
	return o.error != nil && o.retry
}

// IsDone indicates if the operation was successful.
func (o Outcome) IsDone() bool {
	return o.error == nil
}

func (o Outcome) Error() string {
	switch {
	case o.retry:
		return fmt.Sprintf("%v (Retry: %s)", o.error, o.detail)
	case o.error == nil:
		return ""
	default:
		return fmt.Sprintf("%v (Fail: %s)", o.error, o.detail)
	}
}

func (o *Outcome) Unwrap() error {
	return o.error
}

// Failure creates a failure Outcome
func Failure(job tracker.Job, err error, detail string) *Outcome {
	return &Outcome{job: job, error: err, retry: false, detail: detail}
}

// Retry creates a retry type Outcome
func Retry(job tracker.Job, err error, detail string) *Outcome {
	return &Outcome{job: job, error: err, retry: true, detail: detail}
}

// Success returns a successful outcome.
func Success(job tracker.Job, detail string) *Outcome {
	return &Outcome{job: job, detail: detail}
}
