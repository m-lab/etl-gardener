package tracker_test

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m-lab/go/logx"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
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
	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"

	saver := tracker.NewLocalSaver(t.TempDir(), dsKey)
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
	job := tracker.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	url := tracker.UpdateURL(server, job, tracker.Parsing, "foobar")

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Fail if job doesn't exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should update state to Parsing
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job.Key())
	must(t, err)
	if stat.State() != tracker.Parsing {
		t.Fatal("update failed", stat)
	}

	url = tracker.UpdateURL(server, job, tracker.Complete, "")
	postAndExpect(t, url, http.StatusOK)

	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestHeartbeatHandler(t *testing.T) {
	logx.LogxDebug.Set("true")
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := tracker.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	url := tracker.HeartbeatURL(server, job)

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Fail if job doesn't exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should update state to Parsing
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job.Key())
	must(t, err)
	if time.Since(stat.HeartbeatTime) > 1*time.Second {
		t.Fatal("heartbeat failed", stat)
	}
	t.Log(stat)

	url = tracker.UpdateURL(server, job, tracker.Complete, "")
	postAndExpect(t, url, http.StatusOK)

	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestErrorHandler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := tracker.NewJob("bucket", "exp", "type", date)
	server, tk := testSetup(t, []tracker.Job{job})

	url := tracker.ErrorURL(server, job, "error")

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Job should not yet exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should successfully update state to Failed
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job.Key())
	must(t, err)
	if stat.Detail() != "error" {
		t.Error("Expected error:", stat.Detail())
	}
	if stat.State() != tracker.ParseError {
		t.Error("Wrong state:", stat)
	}

	url = tracker.UpdateURL(server, job, tracker.Complete, "")
	postAndExpect(t, url, http.StatusOK)

	_, err = tk.GetStatus(job.Key())
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestNextJobHandler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := tracker.NewJob("bucket", "exp", "type", date)
	// Add job, empty, and duplicate job.
	url, _ := testSetup(t, []tracker.Job{job, tracker.Job{}, job})
	url.Path = "job"

	// Wrong method.
	getAndExpect(t, &url, http.StatusMethodNotAllowed)

	// This should succeed, because the fakeJobService returns its job.
	r := postAndExpect(t, &url, http.StatusOK)
	want := `{"Bucket":"bucket","Experiment":"exp","Datatype":"type","Date":"2019-01-02T00:00:00Z"}`
	if want != r {
		t.Fatalf("/job returned wrong result: got %q, want %q", r, want)
	}

	// This one should fail because the fakeJobService returns empty results.
	postAndExpect(t, &url, http.StatusInternalServerError)

	// This one should fail because the fakeJobService returns a duplicate job.
	postAndExpect(t, &url, http.StatusInternalServerError)
}

func TestNextJobV2Handler(t *testing.T) {
	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := tracker.NewJob("bucket", "exp", "type", date)
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
