package tracker_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

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
			job := tracker.NewJobState("bucket", exp, typ, date)
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
		err := tk.SetJobState(job, tracker.Complete)
		if err != nil {
			t.Error(err, job)
		}
		date = date.Add(24 * time.Hour)
	}
	tk.Sync() // Force synchronous save cycle.
}

func TestTrackerAddDelete(t *testing.T) {
	client := newTestClient()

	tk, err := tracker.InitTracker(context.Background(), client, 0)
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
	restore, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)

	if restore.NumJobs() != 500 {
		t.Fatal("Incorrect number of jobs", restore.NumJobs())
	}

	completeJobs(t, tk, "500Jobs", "type", numJobs)

	tk.Sync()

	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed", tk.NumJobs())
	}

}

// This tests basic Add and update of 2 jobs, and verifies
// correct error returned when trying to update a third job.
func TestUpdate(t *testing.T) {
	client := newTestClient()

	tk, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)

	createJobs(t, tk, "JobToUpdate", "type", 2)
	defer completeJobs(t, tk, "JobToUpdate", "type", 2)

	job := tracker.Job{"bucket", "JobToUpdate", "type", startDate}
	must(t, tk.SetJobState(job, tracker.Parsing))

	must(t, tk.SetJobState(job, tracker.Stabilizing))

	status, err := tk.GetStatus(job)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != tracker.Stabilizing {
		t.Error("Incorrect job state", job)
	}

	err = tk.SetJobState(tracker.Job{"bucket", "JobToUpdate", "nontype", startDate}, tracker.Stabilizing)
	if err != tracker.ErrJobNotFound {
		t.Error(err, "should have been ErrJobNotFound")
	}
}

// This tests whether AddJob and SetJobState generate appropriate
// errors when job doesn't exist.
func TestNonexistentJobAccess(t *testing.T) {
	client := newTestClient()

	tk, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)

	job := tracker.Job{}
	err = tk.SetJobState(job, tracker.Parsing)
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	js := tracker.NewJobState("bucket", "exp", "type", startDate)
	err = tk.AddJob(js)
	if err != nil {
		t.Error(err)
	}

	err = tk.AddJob(js)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	tk.SetJobState(js.Job, tracker.Complete)

	// Job should be gone now.
	err = tk.SetJobState(js.Job, "foobar")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	// The test is intended to exercise job updates at a high
	// rate, and ensure that are no races.
	// It should be run with -race to detect any concurrency
	// problems.
	client := newTestClient()
	// For testing, push to the saver every 5 milliseconds.
	saverInterval := 5 * time.Millisecond
	tk, err := tracker.InitTracker(context.Background(), client, saverInterval)
	must(t, err)

	jobs := 20
	createJobs(t, tk, "ConcurrentUpdates", "type", jobs)
	defer completeJobs(t, tk, "ConcurrentUpdates", "type", jobs)

	changes := 20 * jobs
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(changes)
	for i := 0; i < changes; i++ {
		go func(i int) {
			k := tracker.Job{"bucket", "ConcurrentUpdates", "type",
				startDate.Add(time.Duration(24*rand.Intn(jobs)) * time.Hour)}
			if i%5 == 0 {
				err := tk.SetJobState(k, tracker.State(fmt.Sprintf("State:%d", i)))
				if err != nil {
					log.Fatal(err, " ", k)
				}
			} else {
				err := tk.Heartbeat(k)
				if err != nil {
					log.Fatal(err, " ", k)
				}
			}
			wg.Done()
		}(i)
		time.Sleep(200 * time.Microsecond)
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Error("Expected elapsed time < 2 seconds", elapsed)
	}
}
