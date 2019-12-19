// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"
	job "github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/tracker"
)

func TestService_NextJob(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)

	start := time.Date(2011, 2, 3, 5, 6, 7, 8, time.UTC)
	svc, _ := job.NewJobService(start)
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
	svc, _ := job.NewJobService(start)
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
