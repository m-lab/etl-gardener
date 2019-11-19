// +build integration

package tracker_test

import (
	"context"
	"log"
	"testing"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/datastore/dsiface"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
)

// This test uses an actual datastore client.
// It should generally be run with the datastore emulator.
func TestWithDatastore(t *testing.T) {
	dsc, err := datastore.NewClient(context.Background(), "mlab-testing")
	client := dsiface.AdaptClient(dsc)
	must(t, err)

	tk, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	numJobs := 500
	createJobs(t, tk, "500Jobs", numJobs)
	if tk.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", tk.NumJobs())
	}

	log.Println("Calling Sync")
	must(t, tk.Sync())
	// Check that the sync (and InitTracker) work.
	restore, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)

	if restore.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", restore.NumJobs())
	}

	completeJobs(t, tk, "500Jobs", numJobs)

	tk.Sync()

	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed", tk.NumJobs())
	}

}
