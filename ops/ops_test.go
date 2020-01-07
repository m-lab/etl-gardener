// Package ops provides code that observes the tracker state, and takes appropriate actions.
package ops

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/m-lab/go/logx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestMonitor_Watch(t *testing.T) {
	logx.LogxDebug.Set("true")

	ctx, cancel := context.WithCancel(context.Background())
	tk, err := tracker.InitTracker(ctx, nil, nil, 0)
	rtx.Must(err, "tk init")
	tk.AddJob(tracker.NewJob("bucket", "exp", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type2", time.Now()))

	m := NewMonitor(cloud.BQConfig{})
	m.AddAction(tracker.Init,
		trueCondition,
		newStateFunc(tracker.Parsing),
		"Init")
	m.AddAction(tracker.Parsing,
		trueCondition,
		newStateFunc(tracker.ParseComplete),
		"Stabilizing")
	m.AddAction(tracker.ParseComplete,
		trueCondition,
		newStateFunc(tracker.Stabilizing),
		"Stabilizing")
	m.AddAction(tracker.Stabilizing,
		trueCondition,
		newStateFunc(tracker.Deduplicating),
		"Deduplicating")
	m.AddAction(tracker.Stabilizing,
		trueCondition,
		newStateFunc(tracker.Complete),
		"Complete")
	go m.Watch(ctx, tk, 100*time.Millisecond)

	time.Sleep(2 * time.Second)
	if tk.NumJobs() != 0 {
		t.Error(tk.NumJobs())
	}
	cancel()
}
