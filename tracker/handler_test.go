package tracker_test

import (
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m-lab/go/logx"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/dsfake"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func testSetup(t *testing.T) (url.URL, *tracker.Tracker, tracker.Job) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0, 0, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	// TODO - for now, PDT is ignored by json, so it must be empty.
	job := tracker.NewJob("bucket", "exp", "type", date)
	mux := http.NewServeMux()
	h := tracker.NewHandler(tk)
	h.Register(mux)

	server := httptest.NewServer(mux)
	url, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	return *url, tk, job
}

func getAndExpect(t *testing.T, url *url.URL, code int) {
	resp, err := http.Get(url.String())
	must(t, err)
	if resp.StatusCode != code {
		t.Fatalf("Expected %s, got %s", http.StatusText(code), resp.Status)
	}
	resp.Body.Close()
}

func postAndExpect(t *testing.T, url *url.URL, code int) {
	resp, err := http.Post(url.String(), "application/x-www-form-urlencoded", nil)
	must(t, err)
	if resp.StatusCode != code {
		log.Output(2, resp.Status)
		t.Fatalf("Expected %s, got %s", http.StatusText(code), resp.Status)
	}
	resp.Body.Close()
}

func TestUpdateHandler(t *testing.T) {
	server, tk, job := testSetup(t)

	url := tracker.UpdateURL(server, job, tracker.Parsing, "foobar")

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Fail if job doesn't exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should update state to Parsing
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job)
	must(t, err)
	if stat.State() != tracker.Parsing {
		t.Fatal("update failed", stat)
	}

	url = tracker.UpdateURL(server, job, tracker.Complete, "")
	postAndExpect(t, url, http.StatusOK)

	_, err = tk.GetStatus(job)
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestHeartbeatHandler(t *testing.T) {
	logx.LogxDebug.Set("true")
	server, tk, job := testSetup(t)

	url := tracker.HeartbeatURL(server, job)

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Fail if job doesn't exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should update state to Parsing
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job)
	must(t, err)
	if time.Since(stat.HeartbeatTime) > 1*time.Second {
		t.Fatal("heartbeat failed", stat)
	}
	t.Log(stat)

	url = tracker.UpdateURL(server, job, tracker.Complete, "")
	postAndExpect(t, url, http.StatusOK)

	_, err = tk.GetStatus(job)
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)
	}
}

func TestErrorHandler(t *testing.T) {
	server, tk, job := testSetup(t)

	url := tracker.ErrorURL(server, job, "error")

	getAndExpect(t, url, http.StatusMethodNotAllowed)

	// Job should not yet exist.
	postAndExpect(t, url, http.StatusGone)

	tk.AddJob(job)

	// should successfully update state to Failed
	postAndExpect(t, url, http.StatusOK)
	stat, err := tk.GetStatus(job)
	must(t, err)
	if stat.LastUpdate() != "error" {
		t.Error("Expected error:", stat.LastUpdate())
	}
	if stat.State() != tracker.ParseError {
		t.Error("Wrong state:", stat)
	}
}
