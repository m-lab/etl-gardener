// +build integration

package ops_test

import (
	"context"
	"fmt"
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
	// Not yet supported.
	tk.AddJob(tracker.NewJob("bucket", "exp2", "tcpinfo", time.Now()))
	// Valid experiment and datatype
	// This does an actual dedup, so we need to allow enough time.
	datatypes := []string{"ndt7", "annotation", "pcap", "hopannotation1", "scamper1"}
	for _, datatype := range datatypes {
		tk.AddJob(tracker.NewJob("bucket", "ndt", datatype, time.Now()))
	}

	m, err := ops.NewStandardMonitor(context.Background(), cloud.BQConfig{}, tk)
	rtx.Must(err, "NewMonitor failure")
	// We override some actions in place of the default Parser activity.
	// The resulting sequence should be:
	// init -> parsing -> parse complete -> copying -> deleting -> joining -> complete
	m.AddAction(tracker.Init,
		nil,
		newStateFunc("-"),
		tracker.Parsing)
	m.AddAction(tracker.Parsing,
		nil,
		newStateFunc("-"),
		tracker.ParseComplete)
	// Deliberately skip Load and Dedup functions.
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc("-"),
		tracker.Copying)

	// The rest of the standard state machine picks up from here,
	// with Copying, Joining, ...

	go m.Watch(ctx, 50*time.Millisecond)

	// NOTE: This is an integration test that runs live BQ operations.
	// Therefore, the conditions under which these operations run are not
	// entirely under our control. While the operations are simple, by
	// observation, we see that occassionally they take multiple minutes. So,
	// rather than report these delays as test failures, (which causes the test
	// to be flaky), we wait up to 5min and ignore states pending in the final
	// "Joining" state.
	failTime := time.Now().Add(300 * time.Second)

	for time.Now().Before(failTime) && (tk.NumJobs() > 2 || tk.NumFailed() < 1) {
		time.Sleep(time.Millisecond)
	}
	if tk.NumFailed() != 2 {
		t.Error("Expected NumFailed = 2:", tk.NumFailed())
	}
	// We expect only the two failed jobs; ignore jobs in the final (joining) state.
	count := tk.NumJobs()
	if count > 2 {
		jobs, _, _ := tk.GetState()
		for j, s := range jobs {
			a := m.GetAction(s.LastStateInfo().State)
			switch tracker.State(a.Name()) {
			case tracker.Joining:
				// Ignore delays in state "Joining" (the final step).
				count--
			case tracker.Failed:
				// Ignore -- we expect two.
			default:
				// Consider other states an error.
				t.Error("Monitor.Watch() process delay:", a, j, s)
			}
		}
		fmt.Println()
	}
	// If there are still more than two failed jobs remaining, report an error.
	if count != 2 {
		t.Error("Expected NumJobs = 2:", tk.NumJobs())
	}
	cancel()
}
