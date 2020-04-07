// +build integration

package tracker_test

import (
	"context"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"

	"github.com/m-lab/etl-gardener/tracker"
)

// This test uses an actual datastore client.
// It should generally be run with the datastore emulator.
func TestWithDatastore(t *testing.T) {
	dsc, err := datastore.NewClient(context.Background(), "mlab-testing")
	must(t, err)
	client := dsiface.AdaptClient(dsc)

	dsKey := datastore.NameKey("TestWithDatastore", "jobs", nil)
	dsKey.Namespace = "gardener"
	// NOTE: datastore has eventual consistency, and deletes seem to take
	// quite a while to propogate.
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	numJobs := 500
	createJobs(t, tk, "500Jobs", "type", numJobs)
	if tk.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", tk.NumJobs())
	}

	log.Println("Calling Sync")
	_, err = tk.Sync(time.Time{})
	must(t, err)
	// Check that the sync (and InitTracker) work.
	restore, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0, 0)
	must(t, err)

	if restore.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", restore.NumJobs())
	}

	completeJobs(t, tk, "500Jobs", "type", numJobs)

	_, err = tk.Sync(time.Time{})
	must(t, err)

	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed", tk.NumJobs())
	}
}
