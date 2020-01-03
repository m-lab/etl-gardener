// +build race

package tracker_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/dsfake"
)

func TestConcurrentUpdates(t *testing.T) {
	// The test is intended to exercise job updates at a high
	// rate, and ensure that are no races.
	// It should be run with -race to detect any concurrency
	// problems.
	if testing.Short() {
		t.Skip("Skipping for -short")
	}

	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestConcurrentUpdates", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	// For testing, push to the saver every 5 milliseconds.
	saverInterval := 5 * time.Millisecond
	tk, err := tracker.InitTracker(context.Background(), client, dsKey, saverInterval, 0)
	must(t, err)

	jobs := 20
	createJobs(t, tk, "ConcurrentUpdates", "type", jobs)

	changes := 20 * jobs
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(changes)
	// Execute large number of concurrent updates and heartbeats.
	for i := 0; i < changes; i++ {
		go func(i int) {
			k := tracker.Job{"bucket", "ConcurrentUpdates", "type",
				startDate.Add(time.Duration(24*rand.Intn(jobs)) * time.Hour)}
			if i%5 == 0 {
				err := tk.SetStatus(k, tracker.State(fmt.Sprintf("State:%d", i)), "")
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

	// If verbose, dump the final state.
	if testing.Verbose() {
		must(t, tk.Sync())
		restore, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0)
		must(t, err)

		status := restore.GetAll()
		for k, v := range status {
			log.Println(k, v)
		}
	}
}
