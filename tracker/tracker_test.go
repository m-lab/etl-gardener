// Package tracker tracks status of all jobs, and handles persistence.
//
// Alternative idea for serializing updates to Saver...
//  1. provide a buffered channel to a saver routine for each Job
//  2. send copies of the job to the channel.
//  3. once the channel has the update, further updates are fine.

package tracker_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func createJobs(t *testing.T, tk *tracker.Tracker, prefix string, n int) {
	// Create 100 jobs in parallel
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			err := tk.AddJob(fmt.Sprint(prefix, i))
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	// BUG: There may be Saves still in flight
}

func completeJobs(t *testing.T, tk *tracker.Tracker, prefix string, n int) {
	// Delete all jobs.
	for i := 0; i < n; i++ {
		err := tk.SetJobState(fmt.Sprint(prefix, i), "Complete")
		if err != nil {
			t.Error(err)
		}
	}
	tk.Sync() // Force synchronous save cycle.
}

func TestTrackerAddDelete(t *testing.T) {
	sctx, cf := context.WithCancel(context.Background())
	saver, err := persistence.NewDatastoreSaver(sctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer cf() // This context must be kept alive for life of saver.

	tk, err := tracker.InitTracker(saver, 0)
	if err != nil {
		t.Fatal(err)
	}

	numJobs := 500
	createJobs(t, tk, "500Jobs:", numJobs)
	completeJobs(t, tk, "500Jobs:", numJobs)
}

// This tests basic Add and update of 2 jobs, and verifies
// correct error returned when trying to update a third job.
func TestUpdate(t *testing.T) {
	sctx, cf := context.WithCancel(context.Background())
	saver, err := persistence.NewDatastoreSaver(sctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer cf()

	tk, err := tracker.InitTracker(saver, 0)
	if err != nil {
		t.Fatal(err)
	}

	createJobs(t, tk, "JobToUpdate:", 2)
	defer completeJobs(t, tk, "JobToUpdate:", 2)

	err = tk.SetJobState("JobToUpdate:0", "1")
	if err != nil {
		t.Fatal(err)
	}

	err = tk.SetJobState("JobToUpdate:0", "2")
	if err != nil {
		t.Fatal(err)
	}

	err = tk.SetJobState("JobToUpdate:0", "3")
	if err != nil {
		t.Fatal(err)
	}
}

// This tests whether AddJob and SetJobState generate appropriate
// errors when job doesn't exist.
func TestNonexistentJobAccess(t *testing.T) {
	sctx, cf := context.WithCancel(context.Background())
	saver, err := persistence.NewDatastoreSaver(sctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer cf()

	tk, err := tracker.InitTracker(saver, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = tk.SetJobState("foobar", "non-existent")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	err = tk.AddJob("foobar")
	if err != nil {
		t.Error(err)
	}

	err = tk.AddJob("foobar")
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	tk.SetJobState("foobar", "Complete")
	tk.Sync() // Should cause job cleanup.

	// Job should be gone now.
	err = tk.SetJobState("foobar", "non-existent")
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	// This test should be run with -race to detect any concurrency
	// problems.
	// The test is intended to exercise job updates at a high
	// rate, and ensure that there is not contention across jobs.
	// With cross job contention, the execution time for this test
	// increases dramatically.
	sctx, cf := context.WithCancel(context.Background())
	saver, err := persistence.NewDatastoreSaver(sctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer cf()

	tk, err := tracker.InitTracker(saver, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	jobs := 20
	createJobs(t, tk, "Job:", jobs)
	defer completeJobs(t, tk, "Job:", jobs)

	start := time.Now()
	updates := 20 * jobs
	wg := sync.WaitGroup{}
	wg.Add(updates)
	for i := 0; i < updates; i++ {
		go func(i int) {
			jn := fmt.Sprint("Job:", rand.Intn(jobs))
			if i%5 == 0 {
				err := tk.SetJobState(jn, fmt.Sprint(i))
				if err != nil {
					log.Fatal(err, " ", jn)
				}
			} else {
				err := tk.Heartbeat(jn)
				if err != nil {
					log.Fatal(err, " ", jn)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	if elapsed > 20*time.Second {
		t.Error("There appears to be excessive contention.")
	}
}
