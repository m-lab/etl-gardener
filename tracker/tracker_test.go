package tracker_test

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/datastore/dsiface"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/dsfake"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func must(t *testing.T, err error) {
	if err != nil {
		log.Output(2, err.Error())
		t.Fatal(err)
	}
}

var startDate = time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)

func createJobs(t *testing.T, tk *tracker.Tracker, exp string, typ string, n int) {
	// Create 100 jobs in parallel
	wg := sync.WaitGroup{}
	wg.Add(n)
	date := startDate
	for i := 0; i < n; i++ {
		go func(date time.Time) {
			job := tracker.NewJob("bucket", exp, typ, date)
			err := tk.AddJob(job)
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(date)
		date = date.Add(24 * time.Hour)
	}
	wg.Wait()
}

func completeJobs(t *testing.T, tk *tracker.Tracker, exp string, typ string, n int) {
	// Delete all jobs.
	date := startDate
	for i := 0; i < n; i++ {
		job := tracker.Job{"bucket", exp, typ, date}
		err := tk.SetStatus(job, tracker.Complete, "")

		if err != nil {
			t.Error(err, job)
		}
		date = date.Add(24 * time.Hour)
	}
}

func cleanup(client dsiface.Client, key *datastore.Key) error {
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	err := client.Delete(ctx, key)
	if err != nil && err != datastore.ErrNoSuchEntity {
		tc, ok := client.(*dsfake.Client)
		if ok {
			keys := tc.GetKeys()
			log.Println(keys)
		}
		return err
	}
	return nil
}

func TestJobPath(t *testing.T) {
	withType := tracker.Job{"bucket", "exp", "type", startDate}
	if withType.Path() != "gs://bucket/exp/type/"+startDate.Format("2006/01/02/") {
		t.Error("wrong path:", withType.Path())
	}
	withoutType := tracker.Job{"bucket", "exp", "", startDate}
	if withoutType.Path() != "gs://bucket/exp/"+startDate.Format("2006/01/02/") {
		t.Error("wrong path", withType.Path())
	}
}

func TestTrackerAddDelete(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0)
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
	must(t, tk.Sync())
	// Check that the sync (and InitTracker) work.
	restore, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0)
	must(t, err)

	if restore.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", restore.NumJobs())
	}

	completeJobs(t, tk, "500Jobs", "type", numJobs)

	must(t, tk.Sync())

	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed", tk.NumJobs())
	}
}

// This tests basic Add and update of one jobs, and verifies
// correct error returned when trying to update a non-existent job.
func TestUpdate(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestUpdate", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0)
	must(t, err)

	createJobs(t, tk, "JobToUpdate", "type", 1)

	job := tracker.Job{"bucket", "JobToUpdate", "type", startDate}
	must(t, tk.SetStatus(job, tracker.Parsing, ""))
	must(t, tk.SetStatus(job, tracker.Stabilizing, ""))

	status, err := tk.GetStatus(job)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != tracker.Stabilizing {
		t.Error("Incorrect job state", job)
	}

	err = tk.SetStatus(tracker.Job{"bucket", "JobToUpdate", "other-type", startDate}, tracker.Stabilizing, "")
	if err != tracker.ErrJobNotFound {
		t.Error(err, "should have been ErrJobNotFound")
	}
}

// This tests whether AddJob and SetStatus generate appropriate
// errors when job doesn't exist.
func TestNonexistentJobAccess(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestNonexistentJobAccess", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0)
	must(t, err)

	job := tracker.Job{}
	err = tk.SetStatus(job, tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	err = tk.UpdateJob(job, tracker.NewStatus())
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}

	js := tracker.NewJob("bucket", "exp", "type", startDate)
	must(t, tk.AddJob(js))

	err = tk.AddJob(js)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	must(t, tk.SetStatus(js, tracker.Complete, ""))

	// Job should be gone now.
	err = tk.SetStatus(js, "foobar", "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
}

func TestJobMapHTML(t *testing.T) {
	tk, err := tracker.InitTracker(context.Background(), nil, nil, 0, 0)
	must(t, err)

	job := tracker.Job{}
	err = tk.SetStatus(job, tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	js := tracker.NewJob("bucket", "exp", "type", startDate)
	must(t, tk.AddJob(js))

	buf := bytes.Buffer{}

	if err = tk.WriteHTMLStatusTo(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
}

func TestExpiration(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestExpiration", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	// Expire jobs after 1 second of monkey time.
	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 5*time.Millisecond, 10*time.Millisecond)
	must(t, err)

	job := tracker.NewJob("bucket", "exp", "type", startDate)
	err = tk.SetStatus(job, tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	must(t, tk.AddJob(job))

	err = tk.AddJob(job)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	// Let enough time go by that expirationTime passes, and saver runs.
	time.Sleep(20 * time.Millisecond)

	// Job should have been removed by saveEvery, so this should succeed.
	must(t, tk.AddJob(job))
}
