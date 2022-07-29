package tracker_test

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/m-lab/go/logx"

	gardener "github.com/m-lab/etl-gardener/client/v2"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl-gardener/tracker/jobtest"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// fakeJobService returns a single job from NextJob.
type fakeJobService struct {
	jobs  []tracker.Job
	calls int
}

func (f *fakeJobService) NextJob(ctx context.Context) *tracker.JobWithTarget {
	j := f.jobs[f.calls]
	f.calls++
	return &tracker.JobWithTarget{Job: j}
}

func testSetup(t *testing.T, jobs []tracker.Job) (url.URL, *tracker.Tracker) {
	saver := persistence.NewLocalNamedSaver(path.Join(t.TempDir(), t.Name()+".json"))
	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	mux := http.NewServeMux()

	js := &fakeJobService{jobs: jobs}
	h := tracker.NewHandler(tk, js)
	h.Register(mux)

	server := httptest.NewServer(mux)
	url, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	return *url, tk
}

func getAndExpect(t *testing.T, url *url.URL, code int) {
	resp, err := http.Get(url.String())
	must(t, err)
	if resp.StatusCode != code {
		t.Fatalf("Expected %s, got %s", http.StatusText(code), resp.Status)
	}
	resp.Body.Close()
}

func postAndExpect(t *testing.T, url *url.URL, code int) string {
	resp, err := http.Post(url.String(), "application/x-www-form-urlencoded", nil)
	must(t, err)
	if resp.StatusCode != code {
		log.Output(2, resp.Status)
		t.Fatalf("Expected %s, got %s", http.StatusText(code), resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return string(b)
}

func TestUpdateHandler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := jobtest.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	ctx := context.Background()
	c := gardener.NewJobClient(server)

	// Attempt to update job that is not yet in tracker.
	err := c.Update(ctx, job.Key(), tracker.Init, "foo")
	if err == nil {
		t.Fatal(err)
	}
	err = c.Update(ctx, job.Key(), "", "foo")
	if err == nil {
		t.Fatal(err)
	}
	err = c.Update(ctx, "", tracker.Init, "foo")
	if err == nil {
		t.Fatal(err)
	}

	// Add job to tracker.
	tk.AddJob(job)

	// Update job state.
	err = c.Update(ctx, job.Key(), tracker.Parsing, "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Confirm status.
	stat, err := tk.GetStatus(job.Key())
	must(t, err)
	if stat.State() != tracker.Parsing {
		t.Fatal("update failed", stat)
	}

	// Update job again as complete.
	err = c.Update(ctx, job.Key(), tracker.Complete, "")
	if err != nil {
		t.Fatal(err)
	}

	// Verify job is no longer present.
	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestHeartbeatHandler(t *testing.T) {
	logx.LogxDebug.Set("true")
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := jobtest.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	ctx := context.Background()
	c := gardener.NewJobClient(server)

	// Attempt to send heartbeat for job that is not yet in tracker.
	err := c.Heartbeat(ctx, job.Key())
	if err == nil {
		t.Fatal(err)
	}
	err = c.Heartbeat(ctx, "")
	if err == nil {
		t.Fatal(err)
	}

	// Add job to tracker.
	tk.AddJob(job)

	// Send heartbeat for job.
	err = c.Heartbeat(ctx, job.Key())
	if err != nil {
		t.Fatal(err)
	}

	// Get job status.
	stat, err := tk.GetStatus(job.Key())
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(stat.HeartbeatTime) > 1*time.Second {
		t.Fatal("heartbeat failed", stat)
	}

	// Update job again as complete.
	err = c.Update(ctx, job.Key(), tracker.Complete, "")
	if err != nil {
		t.Fatal(err)
	}

	// Verify job is no longer present.
	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestErrorHandler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := jobtest.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	ctx := context.Background()
	c := gardener.NewJobClient(server)

	// Job should not yet exist.
	err := c.Error(ctx, job.Key(), "no such job")
	if err == nil {
		t.Fatal(err)
	}
	// ID is empty.
	err = c.Error(ctx, "", "")
	if err == nil {
		t.Fatal(err)
	}
	// error message is empty.
	err = c.Error(ctx, job.Key(), "")
	if err == nil {
		t.Fatal(err)
	}

	// Add job to tracker.
	tk.AddJob(job)

	// should successfully update state to Failed
	err = c.Error(ctx, job.Key(), "error")
	if err != nil {
		t.Fatal(err)
	}

	// Verify error is updated in tracker.
	stat, err := tk.GetStatus(job.Key())
	if err != nil {
		t.Fatal(err)
	}
	if stat.Detail() != "error" {
		t.Error("Expected error:", stat.Detail())
	}
	if stat.State() != tracker.ParseError {
		t.Error("Wrong state:", stat)
	}

	err = c.Update(ctx, job.Key(), tracker.Complete, "")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestNextJobV2Handler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := jobtest.NewJob("bucket", "exp", "type", date)
	// Add job, empty, and duplicate job.
	url, _ := testSetup(t, []tracker.Job{job, tracker.Job{}, job})
	url.Path = "/v2/job/next"

	// Wrong method.
	getAndExpect(t, &url, http.StatusMethodNotAllowed)

	// This should succeed, because the fakeJobService returns its job.
	r := postAndExpect(t, &url, http.StatusOK)
	want := `{"ID":"","Job":{"Bucket":"bucket","Experiment":"exp","Datatype":"type","Date":"2019-01-02T00:00:00Z"}}`
	if want != r {
		t.Fatalf("/v2/job/next returned wrong result: got %q, want %q", r, want)
	}

	// This one should fail because the fakeJobService returns empty results.
	postAndExpect(t, &url, http.StatusInternalServerError)

	// This one should fail because the fakeJobService returns a duplicate job.
	postAndExpect(t, &url, http.StatusInternalServerError)
}
