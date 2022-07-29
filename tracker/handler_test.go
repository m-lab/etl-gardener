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
