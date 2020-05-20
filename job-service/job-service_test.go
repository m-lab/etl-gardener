// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"

	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/job-service"
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
	svc, _ := job.NewJobService(nil, start, "fakebucket", sources)
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
	svc, _ := job.NewJobService(nil, start, "fakebucket", sources)
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
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources)
	j := svc.NextJob()
	if j.Date != last.Date {
		t.Error(j, last)
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
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources)

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

func TestYesterday(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	mt := time.Date(2011, 2, 10, 0, 2, 3, 4, time.UTC)

	monkey.Patch(time.Now, func() time.Time {
		return mt
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
	svc, _ := job.NewJobService(tk, start, "fake-bucket", sources)

	// We expect to see "yesterday" interleaved with normal sequence.
	expected := []struct {
		code int
		body string
	}{
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-03T00:00:00Z"}`},
		// Yesterday
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-09T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-09T00:00:00Z"}`},
		// Normal
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-04T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-04T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-05T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-05T00:00:00Z"}`},
		// Yesterday
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-10T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-10T00:00:00Z"}`},
		// Normal
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-06T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-06T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-07T00:00:00Z"}`},

		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-11T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-11T00:00:00Z"}`},

		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-07T00:00:00Z"}`},
		{code: 200, body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-08T00:00:00Z"}`},
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

		// On each cycle, advance the monkey time.
		mt = mt.Add(250 * time.Minute)

	}
}
