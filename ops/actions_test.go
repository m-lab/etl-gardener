package ops_test

import (
	"context"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/osx"
	"github.com/m-lab/go/rtx"
)

// TODO consider rewriting to use a go/cloud/bqfake client.  This is a fair
// bit of work, though.
func TestStandardMonitor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses BQ backend")
	}
	logx.LogxDebug.Set("true")
	cleanup := osx.MustSetenv("PROJECT", "mlab-testing")
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	tk, err := tracker.InitTracker(ctx, nil, nil, 0, 0, 0)
	rtx.Must(err, "tk init")
	tk.AddJob(tracker.NewJob("bucket", "exp", "type", time.Now()))
	// Bad experiment type
	tk.AddJob(tracker.NewJob("bucket", "exp2", "tcpinfo", time.Now()))
	// Valid experiment and datatype
	// This does an actual dedup, so we need to allow enough time.
	tk.AddJob(tracker.NewJob("bucket", "ndt", "annotation", time.Now()))

	m, err := ops.NewStandardMonitor(context.Background(), cloud.BQConfig{}, tk)
	rtx.Must(err, "NewMonitor failure")
	// We add some new actions in place of the Parser activity.
	m.AddAction(tracker.Init,
		nil,
		newStateFunc("-"),
		tracker.Parsing,
		"Init")
	m.AddAction(tracker.Parsing,
		nil,
		newStateFunc("-"),
		tracker.ParseComplete,
		"Parsing")
	// Hack for testing - deliberately skip Load function.
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc("-"),
		tracker.Copying,
		"Copying")

	// The real dedup action should fail on unknown datatype.
	go m.Watch(ctx, 50*time.Millisecond)

	failTime := time.Now().Add(30 * time.Second)

	for time.Now().Before(failTime) && (tk.NumJobs() > 2 || tk.NumFailed() < 2) {
		time.Sleep(time.Millisecond)
	}
	if tk.NumFailed() != 2 {
		t.Error("Expected NumFailed = 2:", tk.NumFailed())
	}
	if tk.NumJobs() != 2 {
		t.Error("Expected NumJobs = 2:", tk.NumJobs())
	}
	cancel()
}
