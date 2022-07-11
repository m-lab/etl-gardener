//go:build integration
// +build integration

package ops_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
)

// TODO consider rewriting to use a go/cloud/bqfake client.  This is a fair
// bit of work, though.
func TestStandardMonitor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses BQ backend")
	}
	logx.LogxDebug.Set("true")

	d := time.Now()
	// Statically define jobs with full configurations specified.
	jobs := []tracker.Job{
		// Not yet supported.
		{
			Bucket:     "bucket",
			Experiment: "exp",
			Datatype:   "type",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_exp", Raw: "raw_exp"},
		},
		// Valid experiment and datatype
		// This does an actual dedup, so we need to allow enough time.
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "ndt7",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "annotation",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "pcap",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "hopannotation1",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "scamper1",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	saver := tracker.NewLocalSaver(t.TempDir(), nil)
	tk, err := tracker.InitTracker(ctx, saver, 0, 0, 0)
	rtx.Must(err, "tk init")

	// Add jobs to the tracker.
	for i := 0; i < len(jobs); i++ {
		tk.AddJob(jobs[i])
	}

	m, err := ops.NewStandardMonitor(context.Background(), "mlab-testing", cloud.BQConfig{}, tk)
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

	for time.Now().Before(failTime) && (tk.NumJobs() > 1 || tk.NumFailed() < 1) {
		time.Sleep(time.Millisecond)
	}
	if tk.NumFailed() != 1 {
		t.Error("Expected NumFailed = 1:", tk.NumFailed())
	}
	// We expect only the two failed jobs; ignore jobs in the final (joining) state.
	count := tk.NumJobs()
	if count > 1 {
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
	if count != 1 {
		t.Error("Expected NumJobs = 1:", tk.NumJobs())
	}
	cancel()
}
