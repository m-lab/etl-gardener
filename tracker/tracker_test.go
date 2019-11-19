package tracker_test

import (
	"context"
	"errors"
	"log"
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

func createJobs(t *testing.T, tk *tracker.Tracker, exp string, n int) {
	// Create 100 jobs in parallel
	wg := sync.WaitGroup{}
	wg.Add(n)
	date := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
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
	date := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
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

type testClient struct {
	dsiface.Client // For unimplemented methods
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
	_, ok := c.objects[*key]
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	delete(c.objects, *key)
	return nil
}

func (c *testClient) Get(ctx context.Context, key *datastore.Key, dst interface{}) (err error) {
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
	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Ptr {
		return nil, datastore.ErrInvalidEntityType
	}
	c.objects[*key] = reflect.Indirect(v)
	return key, nil
}

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

/*

// This tests basic Add and update of 2 jobs, and verifies
// correct error returned when trying to update a third job.
func TestUpdate(t *testing.T) {
	saver := NewTestSaver()

	tk, err := tracker.InitTracker(saver, 0)
	must(t, err)

	createJobs(t, tk, "JobToUpdate:", 2)
	defer completeJobs(t, tk, "JobToUpdate:", 2)

	must(t, tk.SetJobState("JobToUpdate:0", "start"))

	must(t, tk.SetJobState("JobToUpdate:0", "middle"))

	tk.Sync()

	var j = tracker.NewJobState("JobToUpdate:0")
	must(t, saver.Fetch(context.Background(), &j))
	if j.State != "middle" {
		t.Error("Expected State:middle, but", j)
	}

	must(t, tk.SetJobState("JobToUpdate:0", "end"))
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
	// The test is intended to exercise job updates at a high
	// rate, and ensure that are no races.
	// It should be run with -race to detect any concurrency
	// problems.
	saver := NewTestSaver()

	// For testing, push to the saver every 5 milliseconds.
	saverInterval := 5 * time.Millisecond
	tk, err := tracker.InitTracker(saver, saverInterval)
	if err != nil {
		t.Fatal(err)
	}

	jobs := 20
	createJobs(t, tk, "Job:", jobs)
	defer completeJobs(t, tk, "Job:", jobs)

	changes := 20 * jobs
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(changes)
	for i := 0; i < changes; i++ {
		go func(i int) {
			jn := fmt.Sprint("Job:", rand.Intn(jobs))
			if i%5 == 0 {
				err := tk.SetJobState(jn, tracker.State(fmt.Sprintf("State:%d", i)))
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
	elapsed := time.Since(start)
	t.Log(elapsed)
	intervals := int(elapsed / saverInterval)
	maxUpdates := (intervals + 1) * jobs

	// Now we look at all the updates observed by saver.
	total := 0
	for _, job := range saver.GetTasks() {
		total += len(job)
	}
	// Because of heartbeats and updates, we expect most jobs to be
	// saved on each saver interval.  We generally see about 60% of max.
	if total < maxUpdates/2 {
		t.Errorf("Expected at least %d updates, but observed %d\n", maxUpdates/2, total)
	}
}

*/
