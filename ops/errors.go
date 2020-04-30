package ops

import (
	"errors"
	"fmt"

	"github.com/m-lab/etl-gardener/tracker"
)

type Outcome struct {
	job    tracker.Job
	error  // possibly nil
	retry  bool
	detail string
}

// Specific errors for errors.Is
var (
	emptyError = errors.New("")

	ShouldRetry = &Outcome{retry: true, error: emptyError}
	ShouldFail  = &Outcome{retry: false, error: emptyError}
	IsDone      = &Outcome{retry: false}
)

// Is implements errors.Is
func (o *Outcome) Is(target error) bool {
	t, ok := target.(*Outcome)
	if !ok {
		return false
	}
	if o.error == nil {

	}
	return (t.retry == o.retry) &&
		(t.detail == o.detail || t.detail == "")
}

func (o Outcome) Error() string {
	if o.retry {
		return fmt.Sprintf("%v (Retry: %s)", o.error, o.detail)
	}
	return fmt.Sprintf("%v (Fail: %s)", o.error, o.detail)
}

func (o *Outcome) Unwrap() error {
	return o.error
}

// Update uses an outcome to update a job in tracker.
func (o *Outcome) Update(tr *tracker.Tracker, state tracker.State) error {
	if o.error != nil {
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
