package ops_test

import (
	"context"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
)

func TestStandardMonitor(t *testing.T) {
	logx.LogxDebug.Set("true")

	ctx, cancel := context.WithCancel(context.Background())
	tk, err := tracker.InitTracker(ctx, nil, nil, 0, 0)
	rtx.Must(err, "tk init")
	tk.AddJob(tracker.NewJob("bucket", "exp", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type2", time.Now()))

	m, err := ops.NewStandardMonitor(context.Background(), cloud.BQConfig{}, tk)
	rtx.Must(err, "NewMonitor failure")
	// We add some new actions in place of the Parser activity.
	m.AddAction(tracker.Init,
		nil,
		newStateFunc(tracker.Parsing),
		"Init")
	m.AddAction(tracker.Parsing,
		nil,
		newStateFunc(tracker.ParseComplete),
		"Parsing")

	// The real dedup action should fail on unknown datatype.
	go m.Watch(ctx, 10*time.Millisecond)

	failTime := time.Now().Add(2 * time.Second)

	for time.Now().Before(failTime) && tk.NumFailed() < 3 {
	}
	if tk.NumFailed() != 3 {
		t.Error(tk.NumFailed())
	}
	cancel()
}
