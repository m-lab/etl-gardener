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
	"sync"
	"testing"

	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

func TestTrackerAddDelete(t *testing.T) {
	sctx, cf := context.WithCancel(context.Background())
	saver, err := persistence.NewDatastoreSaver(sctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	cf()

	tk, err := tracker.InitTracker(saver)
	if err != nil {
		t.Fatal(err)
	}

	// Create 100 jobs in parallel
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			err := tk.AddJob(fmt.Sprint("Job:", i))
			if err != nil {
				t.Log(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Delete all jobs.
	wg2 := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg2.Add(1)
		go func(i int) {
			err := tk.DeleteJob(fmt.Sprint("Job:", i))
			if err != nil {
				t.Log(err)
			}
			wg2.Done()
		}(i)
	}
	wg2.Wait()

}
