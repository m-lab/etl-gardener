package ops_test

import (
	"errors"
	"strings"
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
	if !strings.Contains(r.Error(), "Retry") {
		t.Error("Should contain Retry:", r)
	}
	if !errors.Is(r, base) {
		t.Error("Should be a base:", r)
	}
	if errors.Is(r, errors.New("other")) {
		t.Error("Should not be other:", r)
	}
}

func TestFail(t *testing.T) {
	job := tracker.Job{}
	base := errors.New("base")
	f := ops.Failure(job, base, "detail")
	if !errors.Is(f, ops.ShouldFail) {
		t.Error(f)
	}
	if errors.Unwrap(f) != base {
		t.Error("Should be base:", errors.Unwrap(f))
	}
	if !strings.Contains(f.Error(), "Fail") {
		t.Error("Should contain Fail:", f)
	}
	if !errors.Is(f, base) {
		t.Error("Should be a base:", f)
	}
	if errors.Is(f, ops.IsDone) {
		t.Error("Should NOT be IsDone:", f)
	}
}

func TestDone(t *testing.T) {
	job := tracker.Job{}
	s := ops.Success(job, "detail")
	if !errors.Is(s, ops.IsDone) {
		t.Error(s)
	}
	if errors.Unwrap(s) != nil {
		t.Error("Should be nil:", errors.Unwrap(s))
	}
}
