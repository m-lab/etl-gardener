// Package ops provides code that observes the tracker state, and takes appropriate actions.
package ops_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func newStateFunc(detail string) ops.ActionFunc {
	return func(ctx context.Context, tk *tracker.Tracker, j tracker.Job) *ops.Outcome {
		return ops.Success(j, detail)
	}
}

func TestMonitor_Watch(t *testing.T) {
	logx.LogxDebug.Set("true")

	ctx, cancel := context.WithCancel(context.Background())
	tk, err := tracker.InitTracker(ctx, nil, nil, 0, 0, 0)
	rtx.Must(err, "tk init")
	tk.AddJob(tracker.NewJob("bucket", "exp", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type2", time.Now()))

	m, err := ops.NewMonitor(context.Background(), cloud.BQConfig{}, tk)
	rtx.Must(err, "NewMonitor failure")
	m.AddAction(tracker.Init,
		nil,
		newStateFunc(""),
		tracker.Parsing,
		"Init")
	m.AddAction(tracker.Parsing,
		nil,
		newStateFunc(""),
		tracker.ParseComplete,
		"Parsing")
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc(""),
		tracker.Stabilizing,
		"PostProcessing")
	m.AddAction(tracker.Stabilizing,
		nil,
		newStateFunc(""),
		tracker.Deduplicating,
		"Checking for stability")
	m.AddAction(tracker.Deduplicating,
		nil,
		newStateFunc(""),
		tracker.Complete,
		"Deduplicating")
	go m.Watch(ctx, 50*time.Millisecond)

	failTime := time.Now().Add(5 * time.Second)

	for time.Now().Before(failTime) && tk.NumJobs() > 0 {
	}
	if tk.NumJobs() != 0 {
		t.Error(tk.NumJobs())
	}
	cancel()
}
