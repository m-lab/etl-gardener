package tracker_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/datastore/dsiface"
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

type testClient struct {
	dsiface.Client // For unimplemented methods
	lock           sync.Mutex
	objects        map[datastore.Key]reflect.Value
}

func newTestClient() *testClient {
	return &testClient{objects: make(map[datastore.Key]reflect.Value, 10)}
}

func (c *testClient) Close() error { return nil }

func (c *testClient) Count(ctx context.Context, q *datastore.Query) (n int, err error) {
	return 0, ErrNotImplemented
}

func (c *testClient) Delete(ctx context.Context, key *datastore.Key) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.objects[*key]
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	delete(c.objects, *key)
	return nil
}

func (c *testClient) Get(ctx context.Context, key *datastore.Key, dst interface{}) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr {
		return datastore.ErrInvalidEntityType
	}
	o, ok := c.objects[*key]
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	v.Elem().Set(o)
	return nil
}

func (c *testClient) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Ptr {
		return nil, datastore.ErrInvalidEntityType
	}
	c.objects[*key] = reflect.Indirect(v)
	return key, nil
}

var startDate = time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)

func createJobs(t *testing.T, tk *tracker.Tracker, exp string, n int) {
	// Create 100 jobs in parallel
	wg := sync.WaitGroup{}
	wg.Add(n)
	date := startDate
	for i := 0; i < n; i++ {
		go func(date time.Time) {
			job := tracker.NewJobState("bucket", exp, date)
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

func completeJobs(t *testing.T, tk *tracker.Tracker, exp string, n int) {
	// Delete all jobs.
	date := startDate
	for i := 0; i < n; i++ {
		key := tracker.JobKey("bucket", exp, date)
		err := tk.SetJobState(key, tracker.Complete)
		if err != nil {
			t.Error(err, key)
		}
		date = date.Add(24 * time.Hour)
	}
	tk.Sync() // Force synchronous save cycle.
}

var ErrNotImplemented = errors.New("Not implemented")

func TestTrackerAddDelete(t *testing.T) {
	client := newTestClient()

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

// This tests basic Add and update of 2 jobs, and verifies
// correct error returned when trying to update a third job.
func TestUpdate(t *testing.T) {
	client := newTestClient()

	tk, err := tracker.InitTracker(context.Background(), client, 0)
	must(t, err)

	createJobs(t, tk, "JobToUpdate", 2)
	defer completeJobs(t, tk, "JobToUpdate", 2)

	prefix := tracker.JobKey("bucket", "JobToUpdate", startDate)
	must(t, tk.SetJobState(prefix, tracker.Parsing))

	must(t, tk.SetJobState(prefix, tracker.Stabilizing))

	job, err := tk.GetJob(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != tracker.Stabilizing {
		t.Error("Incorrect job state", job)
	}

	err = tk.SetJobState("no such job", tracker.Stabilizing)
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

	err = tk.SetJobState("foobar", tracker.Parsing)
	if err != tracker.ErrJobNotFound {
		t.Error("Should be ErrJobNotFound", err)
	}
	job := tracker.NewJobState("foobar", "exp", startDate)
	err = tk.AddJob(job)
	if err != nil {
		t.Error(err)
	}

	err = tk.AddJob(job)
	if err != tracker.ErrJobAlreadyExists {
		t.Error("Should be ErrJobAlreadyExists", err)
	}

	tk.SetJobState(job.Key(), tracker.Complete)

	// Job should be gone now.
	err = tk.SetJobState(job.Key(), "foobar")
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
	createJobs(t, tk, "ConcurrentUpdates", jobs)
	defer completeJobs(t, tk, "ConcurrentUpdates", jobs)

	changes := 20 * jobs
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(changes)
	for i := 0; i < changes; i++ {
		go func(i int) {
			k := tracker.JobKey("bucket", "ConcurrentUpdates",
				startDate.Add(time.Duration(24*rand.Intn(jobs))*time.Hour))
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
