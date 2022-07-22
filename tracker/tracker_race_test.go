//go:build race
// +build race

package tracker_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

func TestConcurrentUpdates(t *testing.T) {
	// The test is intended to exercise job updates at a high
	// rate, and ensure that are no races.
	// It should be run with -race to detect any concurrency
	// problems.
	if testing.Short() {
		t.Skip("Skipping for -short")
	}

	ctx := context.Background()

	// NOTE: we use os.MkdirTemp instead of t.TempDir() because during -race
	// tests the directory is removed mid test, corrupting behavior of the test.
	dir, _ := os.MkdirTemp("tmp", "*")
	saver := persistence.NewLocalNamedSaver(path.Join(dir, t.Name()+".json"))

	// For testing, push to the saver every 5 milliseconds.
	saverInterval := 5 * time.Millisecond
	tk, err := tracker.InitTracker(ctx, saver, saver, saverInterval, 0, 0)
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
			k := tracker.Job{
				Bucket:     "bucket",
				Experiment: "ConcurrentUpdates",
				Datatype:   "type",
				Date:       startDate.Add(time.Duration(24*rand.Intn(jobs)) * time.Hour),
			}
			if i%5 == 0 {
				err := tk.SetStatus(k.Key(), tracker.State(fmt.Sprintf("State:%d", i)), "")
				if err != nil {
					log.Fatal(err, " ", k)
				}
			} else {
				err := tk.Heartbeat(k.Key())
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
		_, err := tk.Sync(ctx, time.Time{})
		must(t, err)
		restore, err := tracker.InitTracker(context.Background(), saver, saver, 0, 0, 0)
		must(t, err)

		status, _, _ := restore.GetState()
		for k, v := range status {
			log.Println(k, v)
		}
	}
}
