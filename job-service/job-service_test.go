// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"
	job "github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func TestService_NextJob(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)

	start := time.Date(2011, 2, 3, 5, 6, 7, 8, time.UTC)
	svc, _ := job.NewJobService(nil, start)
	j := svc.NextJob()
	w := tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "ndt5", Date: start.Truncate(24 * time.Hour)}
	diff := deep.Equal(w, j)
	if diff != nil {
		t.Fatal(diff)
	}
	j = svc.NextJob()
	w = tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "tcpinfo", Date: start.Truncate(24 * time.Hour)}
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Fatal(diff)
	}
	j = svc.NextJob()
	w = tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "ndt5", Date: start.Add(24 * time.Hour).Truncate(24 * time.Hour)}
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Fatal(diff)
	}
	j = svc.NextJob()
	w = tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "tcpinfo", Date: start.Add(24 * time.Hour).Truncate(24 * time.Hour)}
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Fatal(diff)
	}
	// Wrap
	j = svc.NextJob()
	w = tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "ndt5", Date: start.Truncate(24 * time.Hour)}
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Fatal(diff)
	}
}

func TestJobHandler(t *testing.T) {
	start := time.Date(2011, 2, 3, 5, 6, 7, 8, time.UTC)
	svc, _ := job.NewJobService(nil, start)
	req := httptest.NewRequest("", "/job", nil)
	resp := httptest.NewRecorder()
	svc.JobHandler(resp, req)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Error("Should be MethodNotAllowed", http.StatusText(resp.Code))
	}

	req = httptest.NewRequest("POST", "/job", nil)
	resp = httptest.NewRecorder()
	svc.JobHandler(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatal(resp.Code)
	}

	want := `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`
	if want != resp.Body.String() {
		t.Fatal(resp.Body.String())
	}
}

func TestEarlyWrapping(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)
	start := time.Date(2011, 2, 3, 5, 6, 7, 8, time.UTC)
	tk, err := tracker.InitTracker(context.Background(), nil, nil, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}

	svc, _ := job.NewJobService(tk, start)

	// If a job is still present in the tracker when it wraps, /job returns an error.
	results := []struct {
		code int
		body string
	}{
		{code: 200, body: `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-03T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-04T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-04T00:00:00Z"}`},
		// This one should work, because we complete it in the loop.
		{code: 200, body: `{"Bucket":"archive-mlab-sandbox","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{code: 500, body: `Job already exists.  Try again.`},
	}

	for k, result := range results {
		req := httptest.NewRequest("POST", "/job", nil)
		resp := httptest.NewRecorder()
		svc.JobHandler(resp, req)
		if resp.Code != result.code {
			t.Fatal(k, resp.Code, resp.Body.String())
		}
		if resp.Body.String() != result.body {
			t.Fatal(k, resp.Body.String())
		}

		if k == 2 {
			job := tracker.Job{}
			json.Unmarshal([]byte(results[0].body), &job)
			err := tk.UpdateJob(job, tracker.Status{State: tracker.Complete})
			if err != nil {
				t.Error(err)
			}
		}
	}
}

type fakeGardener struct {
	t *testing.T // for logging

	lock       sync.Mutex
	jobs       []tracker.Job
	heartbeats int
	updates    int
}

func (g *fakeGardener) AddJob(job tracker.Job) {
	g.jobs = append(g.jobs, job)
}

func (g *fakeGardener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rtx.Must(r.ParseForm(), "bad request")
	if r.Method != http.MethodPost {
		log.Fatal("Should be POST") // Not t.Fatal because this is asynchronous.
	}
	g.lock.Lock()
	g.lock.Unlock()
	switch r.URL.Path {
	case "/job":
		if len(g.jobs) < 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		j := g.jobs[0]
		g.jobs = g.jobs[1:]
		w.Write(j.Marshal())
	case "/heartbeat":
		g.t.Log(r.URL.Path, r.URL.Query())
		g.heartbeats++

	case "/update":
		g.t.Log(r.URL.Path, r.URL.Query())
		g.updates++

	default:
		log.Fatal(r.URL) // Not t.Fatal because this is asynchronous.
	}
}

func TestJobClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a fake gardener service.
	fg := fakeGardener{t: t, jobs: make([]tracker.Job, 0)}
	fg.AddJob(tracker.NewJob("foobar", "ndt", "ndt5", time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC)))
	gardener := httptest.NewServer(&fg)
	defer gardener.Close()
	gURL, err := url.Parse(gardener.URL)
	rtx.Must(err, "bad url")

	j, err := job.NextJob(ctx, *gURL)
	rtx.Must(err, "next job")

	if j.Path() != "gs://foobar/ndt/ndt5/2019/01/01/" {
		t.Error(j.Path())
	}

	j, err = job.NextJob(ctx, *gURL)
	if err.Error() != "Internal Server Error" {
		t.Fatal("Should be internal server error", err)
	}
}
