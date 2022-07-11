package client_test

import (
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/client"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl-gardener/tracker/jobtest"
	"github.com/m-lab/go/rtx"
)

type fakeGardener struct {
	t *testing.T // for logging

	lock       sync.Mutex
	jobs       []tracker.JobWithTarget
	heartbeats int
	updates    int
}

func (g *fakeGardener) AddJob(job tracker.Job, target string) error {
	jt := tracker.JobWithTarget{
		ID:  job.Key(),
		Job: job,
	}
	g.jobs = append(g.jobs, jt)
	return nil
}

func (g *fakeGardener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rtx.Must(r.ParseForm(), "bad request")
	if r.Method != http.MethodPost {
		log.Fatal("Should be POST") // Not t.Fatal because this is asynchronous.
	}
	g.lock.Lock()
	defer g.lock.Unlock()
	switch r.URL.Path {
	case "/job":
		if len(g.jobs) < 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		j := g.jobs[0]
		g.jobs = g.jobs[1:]
		w.Write(j.Job.Marshal())
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
	fg := fakeGardener{t: t, jobs: make([]tracker.JobWithTarget, 0)}
	spec := jobtest.NewJob(
		"foobar", "ndt", "ndt5", time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC))
	rtx.Must(fg.AddJob(spec, "a.b.c"), "add job")
	gardener := httptest.NewServer(&fg)
	defer gardener.Close()
	gURL, err := url.Parse(gardener.URL)
	rtx.Must(err, "bad url")

	j, err := client.NextJob(ctx, *gURL)
	rtx.Must(err, "next job")

	if j.Job.Path() != "gs://foobar/ndt/ndt5/2019/01/01/" {
		t.Error(j.Job.Path())
	}

	j, err = client.NextJob(ctx, *gURL)
	if err.Error() != "Internal Server Error" {
		t.Fatal("Should be internal server error", err)
	}
}
