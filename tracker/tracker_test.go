package tracker_test

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker/jobtest"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"

	"github.com/m-lab/go/cloudtest/dsfake"
	"github.com/m-lab/go/logx"

	"github.com/m-lab/etl-gardener/tracker"
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
			job := jobtest.NewJob(
				"bucket", exp, typ, date,
			)
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
		job := jobtest.NewJob(
			"bucket", exp, typ, date,
		)
		err := tk.SetStatus(job.Key(), tracker.Complete, "")

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
	withType := tracker.Job{
		Bucket: "bucket", Experiment: "exp", Datatype: "type", Date: startDate, Filter: "",
	}
	if withType.Path() != "gs://bucket/exp/type/"+startDate.Format("2006/01/02/") {
		t.Error("wrong path:", withType.Path())
	}
	withoutType := tracker.Job{
		Bucket: "bucket", Experiment: "exp", Date: startDate, Filter: "",
	}
	if withoutType.Path() != "gs://bucket/exp/"+startDate.Format("2006/01/02/") {
		t.Error("wrong path", withType.Path())
	}
}

func TestTrackerAddDelete(t *testing.T) {
	ctx := context.Background()
	logx.LogxDebug.Set("true")

	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"
	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)

	tk, err := tracker.InitTracker(ctx, saver, 0, 0, time.Second)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	numJobs := 100
	createJobs(t, tk, "100Jobs", "type", numJobs)
	if tk.NumJobs() != 100 {
		t.Fatal("Incorrect number of jobs", tk.NumJobs())
	}

	log.Println("Calling Sync")
	if _, err := tk.Sync(ctx, time.Time{}); err != nil {
		must(t, err)
	}
	// Check that the sync (and InitTracker) work.
	// Jobs will be removed by GetStatus 50 milliseconds after Complete.
	restore, err := tracker.InitTracker(context.Background(), saver, 0, 0, 50*time.Millisecond)
	must(t, err)

	if restore.NumJobs() != 100 {
		t.Fatal("Incorrect number of jobs", restore.NumJobs())
	}

	if tk.NumFailed() != 0 {
		t.Error("Should not be any failed jobs")
	}

	completeJobs(t, tk, "100Jobs", "type", numJobs)

	// This tests proper behavior of cleanup with cleanupDelay.
	jobs, _, _ := tk.GetState()
	if len(jobs) < numJobs {
		t.Error("Too few jobs:", len(jobs), "<", numJobs)
	}
	active := 0
	for _, s := range jobs {
		if s.State() != tracker.Complete {
			active++
		}
	}
	if active > 0 {
		t.Error("Should be zero active jobs")
	}

	// It will take up to 50 milliseconds to delete the jobs.
	deadline := time.Now().Add(time.Second)
	for tk.NumJobs() != 0 && time.Since(deadline) < 0 {
		time.Sleep(time.Millisecond)
		tk.GetState()
	}
	if _, err := tk.Sync(ctx, time.Time{}); err != nil {
		must(t, err)
	}

	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed", tk.NumJobs())
	}

}

func TestUpdates(t *testing.T) {
	dsKey := datastore.NameKey("TestUpdate", "jobs", nil)
	dsKey.Namespace = "gardener"
	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)

	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0)
	must(t, err)

	createJobs(t, tk, "JobToUpdate", "type", 1)

	job := tracker.Job{
		Bucket: "bucket", Experiment: "JobToUpdate", Datatype: "type", Date: startDate, Filter: "",
	}
	key := job.Key()
	must(t, tk.SetStatus(key, tracker.Parsing, "foo"))
	must(t, tk.SetStatus(key, tracker.Stabilizing, "bar"))

	status, err := tk.GetStatus(key)
	must(t, err)
	if status.State() != tracker.Stabilizing {
		t.Error("Incorrect job state", job)
	}

	must(t, tk.SetDetail(key, "foobar"))
	status, err = tk.GetStatus(key)
	must(t, err)
	if status.Detail() != "foobar" {
		t.Error("Incorrect detail", status.LastStateInfo())
	}
	j := tracker.Job{
		Bucket: "bucket", Experiment: "JobToUpdate", Datatype: "other-type", Date: startDate, Filter: "",
	}
	err = tk.SetStatus(j.Key(), tracker.Stabilizing, "")
	if err != tracker.ErrJobNotFound {
		t.Error(err, "should have been ErrJobNotFound")
	}

	if tk.NumFailed() != 0 {
		t.Fatal("NumFailed should be 0", tk.NumFailed())
	}

	must(t, tk.SetJobError(key, "Fake Error"))
	if tk.NumFailed() != 1 {
		t.Fatal("NumFailed should be 1", tk.NumFailed())
	}
}

// This tests whether AddJob and SetStatus generate appropriate
// errors when job doesn't exist.
func TestNonexistentJobAccess(t *testing.T) {
	dsKey := datastore.NameKey("TestNonexistentJobAccess", "jobs", nil)
	dsKey.Namespace = "gardener"
	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)

	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0)
	must(t, err)

	job := tracker.Job{}
	key := job.Key()
	err = tk.SetStatus(key, tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	err = tk.UpdateJob(key, tracker.NewStatus())
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}

	js := jobtest.NewJob("bucket", "exp", "type", startDate)
	must(t, tk.AddJob(js))

	err = tk.AddJob(js)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	must(t, tk.SetStatus(js.Key(), tracker.Complete, ""))

	// Job should be gone now.
	err = tk.SetStatus(js.Key(), "foobar", "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
}

func TestJobMapHTML(t *testing.T) {
	dsKey := datastore.NameKey("TestJobMapHTML", "jobs", nil)
	dsKey.Namespace = "gardener"
	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)

	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0)
	must(t, err)

	job := tracker.Job{}
	err = tk.SetStatus(job.Key(), tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	js := jobtest.NewJob("bucket", "exp", "type", startDate)
	must(t, tk.AddJob(js))

	buf := bytes.Buffer{}

	if err = tk.WriteHTMLStatusTo(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
}

func TestExpiration(t *testing.T) {
	dsKey := datastore.NameKey("TestExpiration", "jobs", nil)
	dsKey.Namespace = "gardener"
	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)

	ctx, cancel := context.WithCancel(context.Background())

	// Expire jobs after 1 second of monkey time.
	tk, err := tracker.InitTracker(ctx, saver, 5*time.Millisecond, 10*time.Millisecond, 1*time.Millisecond)
	must(t, err)

	job := jobtest.NewJob("bucket", "exp", "type", startDate)
	err = tk.SetStatus(job.Key(), tracker.Parsing, "")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	must(t, tk.AddJob(job))

	err = tk.AddJob(job)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	// Let enough time go by that expirationTime passes, and saver runs.
	time.Sleep(40 * time.Millisecond)

	// Job should have been removed by saveEvery, so this should succeed.
	must(t, tk.AddJob(job))

	// Stop saveEvery go routine, so cleanup will remove file.
	cancel()
	time.Sleep(40 * time.Millisecond)
}
