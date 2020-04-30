package ops_test

import (
	"errors"
	"testing"

	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
)

func TestRetry(t *testing.T) {
	job := tracker.Job{}
	base := errors.New("base")
	r := ops.Retry(job, base, "detail")
	if !errors.Is(r, ops.ShouldRetry) {
		t.Error(r)
	}
	if errors.Unwrap(r) != base {
		t.Error("Should be base:", errors.Unwrap(r))
	}
}

func TestFail(t *testing.T) {
	job := tracker.Job{}
	base := errors.New("base")
	r := ops.Fail(job, base, "detail")
	if !errors.Is(r, ops.ShouldFail) {
		t.Error(r)
	}
	if errors.Unwrap(r) != base {
		t.Error("Should be base:", errors.Unwrap(r))
	}
}

func TestDone(t *testing.T) {
	job := tracker.Job{}
	r := ops.Done(job, "detail")
	if !errors.Is(r, ops.IsDone) {
		t.Error(r)
	}
	if errors.Unwrap(r) != nil {
		t.Error("Should be nil:", errors.Unwrap(r))
	}
}
