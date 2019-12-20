package tracker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/dsfake"
)

func TestUpdateHandler(t *testing.T) {
	client := dsfake.NewClient()
	dsKey := datastore.NameKey("TestTrackerAddDelete", "jobs", nil)
	dsKey.Namespace = "gardener"
	defer must(t, cleanup(client, dsKey))

	tk, err := tracker.InitTracker(context.Background(), client, dsKey, 0)
	must(t, err)
	if tk == nil {
		t.Fatal("nil Tracker")
	}

	date := time.Date(2019, 01, 02, 0, 0, 0, 0, time.UTC)
	job := tracker.NewJob("bucket", "exp", "type", date)
	mux := http.NewServeMux()
	h := tracker.NewHandler(tk)
	h.Register(mux)

	server := httptest.NewServer(mux)

	url, err := tracker.UpdateURL(server.URL, job, tracker.Parsing, "foobar")
	must(t, err)

	resp, err := http.Get(url)
	must(t, err)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatal("Expected Method Not Allowed", resp.Status)
	}
	resp.Body.Close()

	// Fail if job doesn't exist.
	resp, err = http.Post(url, "application/x-www-form-urlencoded", nil)
	must(t, err)
	if resp.StatusCode != http.StatusGone {
		t.Fatal("Expected StatusGone", resp.Status)
	}
	resp.Body.Close()

	tk.AddJob(job)
	must(t, err)

	resp, err = http.Post(url, "application/x-www-form-urlencoded", nil)
	must(t, err)
	if resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	resp.Body.Close()

	stat, err := tk.GetStatus(job)
	must(t, err)
	if stat.State != tracker.Parsing {
		t.Fatal("update failed", stat)
	}

	url, err = tracker.UpdateURL(server.URL, job, tracker.Complete, "")
	must(t, err)
	resp, err = http.Post(url, "application/x-www-form-urlencoded", nil)
	must(t, err)
	if resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	resp.Body.Close()

	_, err = tk.GetStatus(job)
	if err != tracker.ErrJobNotFound {
		t.Fatal("Expected JobNotFound", err)

	}
}
