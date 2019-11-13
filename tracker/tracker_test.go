// Package tracker tracks status of all jobs, and handles persistence.
//
// Alternative idea for serializing updates to Saver...
//  1. provide a buffered channel to a saver routine for each Job
//  2. send copies of the job to the channel.
//  3. once the channel has the update, further updates are fine.

package tracker_test

import (
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
	saver := NewTestSaver()

	tk, err := tracker.InitTracker(saver, 0)
	if err != nil {
		t.Fatal(err)
	}

	numJobs := 500
	createJobs(t, tk, "500Jobs:", numJobs)
	completeJobs(t, tk, "500Jobs:", numJobs)
	if tk.NumJobs() != 0 {
		t.Error("Job cleanup failed")
	}

	all := saver.GetTasks()
	for _, states := range all {
		final := states[len(states)-1]
		if final.Name != "" {
			t.Error(final)
		}
	}
}

// This tests basic Add and update of 2 jobs, and verifies
// correct error returned when trying to update a third job.
func TestUpdate(t *testing.T) {
	saver := NewTestSaver()

	tk, err := tracker.InitTracker(saver, 0)
	if err != nil {
		t.Fatal(err)
	}

	createJobs(t, tk, "JobToUpdate:", 2)
	defer completeJobs(t, tk, "JobToUpdate:", 2)

	err = tk.SetJobState("JobToUpdate:0", "start")
	if err != nil {
		t.Fatal(err)
	}

	err = tk.SetJobState("JobToUpdate:0", "middle")
	if err != nil {
		t.Fatal(err)
	}

	err = tk.SetJobState("JobToUpdate:0", "end")
	if err != nil {
		t.Fatal(err)
	}
}

// This tests whether AddJob and SetJobState generate appropriate
// errors when job doesn't exist.
func TestNonexistentJobAccess(t *testing.T) {
	saver := NewTestSaver()

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
	saver := NewTestSaver()

	// For testing, push to the saver every 5 milliseconds.
	tk, err := tracker.InitTracker(saver, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	jobs := 20
	createJobs(t, tk, "Job:", jobs)
	defer completeJobs(t, tk, "Job:", jobs)

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
		time.Sleep(200 * time.Microsecond)
	}
	wg.Wait()

	max := 0
	min := 100
	for _, job := range saver.GetTasks() {
		if max < len(job) {
			max = len(job)
		}
		if min > len(job) {
			min = len(job)
		}
	}
	if min < 5 {
		t.Error("Warning, expected at least 5 updates per job, saw", min)
	}
	if max < 15 {
		t.Error("Warning, expected > 15 updates for at least one job, saw", max)
	}
}
