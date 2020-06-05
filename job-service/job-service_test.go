// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"

	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestService_NextJob(t *testing.T) {

	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	svc, _ := job.NewJobService(nil, start, "fakebucket", sources, nil)
	j := svc.NextJob()
	w, err := tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Date: start.Truncate(24 * time.Hour)}.Target("fakebucket.tmp_ndt.ndt5")
	rtx.Must(err, "")
	diff := deep.Equal(w, j)
	if diff != nil {
		t.Error(diff)
	}
	j = svc.NextJob()
	w, err = tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Date: start.Truncate(24 * time.Hour)}.Target("fakebucket.tmp_ndt.tcpinfo")
	rtx.Must(err, "")
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Error(diff)
	}
	j = svc.NextJob()
	w, err = tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Date: start.Add(24 * time.Hour).Truncate(24 * time.Hour)}.Target("fakebucket.tmp_ndt.ndt5")
	rtx.Must(err, "")
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Error(diff)
	}
	j = svc.NextJob()
	w, err = tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Date: start.Add(24 * time.Hour).Truncate(24 * time.Hour)}.Target("fakebucket.tmp_ndt.tcpinfo")
	rtx.Must(err, "")
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Error(diff)
	}
	// Wrap
	j = svc.NextJob()
	w, err = tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Date: start.Truncate(24 * time.Hour)}.Target("fakebucket.tmp_ndt.ndt5")
	rtx.Must(err, "")
	diff = deep.Equal(w, j)
	if diff != nil {
		t.Error(diff)
	}
}

func TestJobHandler(t *testing.T) {
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	svc, _ := job.NewJobService(nil, start, "fakebucket", sources, nil)
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

	want := `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`
	if want != resp.Body.String() {
		t.Fatal(resp.Body.String())
	}
}

func TestResume(t *testing.T) {
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	tk, err := tracker.InitTracker(context.Background(), nil, nil, 0, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}
	lastJobDate := start.AddDate(0, 0, 3)
	last := tracker.NewJob("fake-bucket", "ndt", "ndt5", lastJobDate)
	tk.AddJob(last)

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources, nil)
	j := svc.NextJob()
	if j.Date != last.Date {
		t.Error(j, last)
	}
}

// Implements persistence.Saver, for test injection.
type saver struct {
	Date time.Time
}

func (s *saver) Save(ctx context.Context, o persistence.StateObject) error {
	switch svc := o.(type) {
	case *job.Service:
		s.Date = svc.Date
	default:
		os.Exit(1)
	}
	return nil
}
func (s *saver) Delete(ctx context.Context, o persistence.StateObject) error {
	return nil
}
func (s *saver) Fetch(ctx context.Context, o persistence.StateObject) error {
	switch s := o.(type) {
	case *job.Service:
		s.Date =
			time.Date(2011, 2, 13, 0, 0, 0, 0, time.UTC)
	default:
		os.Exit(1)
	}
	return nil
}

func assertStateObject(so persistence.StateObject) {
	assertStateObject(&job.Service{})
}

func TestResumeFromSaver(t *testing.T) {
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	tk, err := tracker.InitTracker(context.Background(), nil, nil, 0, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	ss := saver{}
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources, &ss)
	j := svc.NextJob()
	if j.Date != start.AddDate(0, 0, 10) {
		t.Error("Expected 20110213", j)
	}

	// Now check save invocation...
	j = svc.NextJob()
	if ss.Date.Before(start.AddDate(0, 0, 11)) {
		t.Error("Expected 20110214", ss.Date)
	}
}

func TestEarlyWrapping(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	tk, err := tracker.InitTracker(context.Background(), nil, nil, 0, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources, nil)

	// If a job is still present in the tracker when it wraps, /job returns an error.
	expected := []struct {
		code int
		body string
	}{
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-03T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-04T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-04T00:00:00Z"}`},
		// This one should work, because we complete it in the loop.
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{code: 500, body: `Job already exists.  Try again.`},
	}

	for k, result := range expected {
		req := httptest.NewRequest("POST", "/job", nil)
		resp := httptest.NewRecorder()
		svc.JobHandler(resp, req)
		if resp.Code != result.code {
			t.Error(k, resp.Code, resp.Body.String())
		}
		if resp.Body.String() != result.body {
			t.Error(k, "Got:", resp.Body.String(), "!=", result.body)
		}

		if k == 2 {
			job := tracker.Job{}
			json.Unmarshal([]byte(expected[0].body), &job)
			status, _ := tk.GetStatus(job)
			status.Update(tracker.Complete, "")
			err := tk.UpdateJob(job, status)
			if err != nil {
				t.Error(err)
			}
		}
	}
}
