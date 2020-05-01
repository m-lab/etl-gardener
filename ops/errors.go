package ops

import (
	"errors"
	"fmt"

	"github.com/m-lab/etl-gardener/tracker"
)

// Outcome is a custom error for use in this package.
type Outcome struct {
	job    tracker.Job
	error  // possibly nil
	retry  bool
	detail string
}

// Specific errors for errors.Is
var (
	errEmpty = errors.New("")

	ShouldRetry = &Outcome{retry: true, error: errEmpty}
	ShouldFail  = &Outcome{retry: false, error: errEmpty}
	IsDone      = &Outcome{retry: false}
)

// Is implements errors.Is
func (o *Outcome) Is(target error) bool {
	t, ok := target.(*Outcome)
	if !ok {
		return false
	}
	if (o.error == nil) != (t.error == nil) {
		return false
	}
	return (t.retry == o.retry) &&
		(t.detail == o.detail || t.detail == "")
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

// Update uses an outcome to update a job in tracker.
func (o *Outcome) Update(tr *tracker.Tracker, state tracker.State) error {
	if errors.Is(o, ShouldFail) {
		return tr.SetJobError(o.job, o.detail) // TODO - is this correct?
	}
	return tr.SetStatus(o.job, state, o.detail)
}

// Failure creates a failure Outcome
func Failure(job tracker.Job, err error, detail string) *Outcome {
	return &Outcome{job, err, false, detail}
}

// Retry creates a retry type Outcome
func Retry(job tracker.Job, err error, detail string) *Outcome {
	return &Outcome{job, err, true, detail}
}

// Success returns a successful outcome.
func Success(job tracker.Job, detail string) *Outcome {
	return &Outcome{job: job, detail: detail}
}
