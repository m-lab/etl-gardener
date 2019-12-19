package tracker_test

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/dsfake"
)

func TestHeartbeat(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	numJobs := 5
	createJobs(t, tk, "5Jobs", "type", numJobs)
	if tk.NumJobs() != 5 {
		t.Fatal("Incorrect number of jobs", tk.NumJobs())
	}

	tracker.NewHandler(tk)
}
