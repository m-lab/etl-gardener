// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func must(t *testing.T, err error) {
	if err != nil {
		log.Output(2, err.Error())
		t.Fatal(err)
	}
}

type NullTracker struct{}

func (nt *NullTracker) AddJob(job tracker.Job) error {
	return nil
}

func (nt *NullTracker) LastJob() tracker.Job {
	return tracker.Job{}
}

type NullSaver struct {
}

func (s *NullSaver) Save(ctx context.Context, o persistence.StateObject) error {
	return errors.New("Null Saver")
}
func (s *NullSaver) Delete(ctx context.Context, o persistence.StateObject) error {
	return errors.New("Null Saver")
}
func (s *NullSaver) Fetch(ctx context.Context, o persistence.StateObject) error {
	return errors.New("Null Saver")
}

func TestService_NextJob(t *testing.T) {
	// This allows predictable behavior from time.Since in the advanceDate function.
	// It will cause wrapping when the service would advance from 2011/2/4 to
	// 2011/2/5, since 2/5 is less than 36 hours prior to "now"
	now := time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	monkey.Patch(time.Now, func() time.Time {
		return now
	})
	defer monkey.Unpatch(time.Now)

	ctx := context.Background()

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	// This is three days before "now".  The job service should restart
	// when it reaches 36 hours before "now", which is 2011-02-05
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	svc, err := job.NewJobService(&NullTracker{}, start, "fakebucket", sources, &NullSaver{})
	must(t, err)

	expected := []struct {
		body string
	}{
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-03T00:00:00Z"}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-04T00:00:00Z"}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-04T00:00:00Z"}`},
		// Wrap
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z"}`},
	}

	for i, e := range expected {
		want := tracker.Job{}
		json.Unmarshal([]byte(e.body), &want)
		got := svc.NextJob(ctx)
		log.Println(got)
		diff := deep.Equal(want, got.Job)
		if diff != nil {
			t.Error(i, diff)
		}
	}
}

func TestJobHandler(t *testing.T) {
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	svc, err := job.NewJobService(&NullTracker{}, start, "fakebucket", sources, &NullSaver{})
	must(t, err)
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
	ctx := context.Background()

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
	svc, err := job.NewJobService(tk, start, "fake-bucket", sources, &NullSaver{})
	must(t, err)
	j := svc.NextJob(ctx)
	if j.Date != last.Date {
		t.Error(j, last)
	}
}

// Implements persistence.Saver, for test injection.
type FakeSaver struct {
	Date time.Time
}

func (fs *FakeSaver) Save(ctx context.Context, o persistence.StateObject) error {
	switch svc := o.(type) {
	case *job.Service:
		fs.Date = svc.Date
	default:
		log.Fatal("Not implemented")
	}
	return nil
}
func (fs *FakeSaver) Delete(ctx context.Context, o persistence.StateObject) error {
	return nil
}
func (fs *FakeSaver) Fetch(ctx context.Context, o persistence.StateObject) error {
	switch to := o.(type) {
	case *job.Service:
		to.Date = fs.Date
	default:
		log.Fatal("Not implemented")
	}
	return nil
}

func assertStateObject(so persistence.StateObject) {
	assertStateObject(&job.Service{})
}

func TestResumeFromSaver(t *testing.T) {
	ctx := context.Background()

	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Target: "tmp_ndt.ndt5"},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Target: "tmp_ndt.tcpinfo"},
	}
	// FakeSaver that will return 2011/02/13 as resume date.
	resume := time.Date(2011, 2, 13, 0, 0, 0, 0, time.UTC)
	fs := FakeSaver{Date: resume}
	svc, err := job.NewJobService(&NullTracker{}, start, "fake-bucket", sources, &fs)
	must(t, err)
	// NextJob should return a job with date provided by FakeSaver.
	j := svc.NextJob(ctx)
	if j.Date != resume {
		t.Error("Expected ", resume, "got", j)
	}

	// When NextJob returns the last job for the date, it should trigger
	// saving the next date to process, which is 20110214.
	j = svc.NextJob(ctx)
	// Check that we see the new date in the FakeSaver
	if fs.Date != resume.AddDate(0, 0, 1) {
		t.Error("Expected", resume.AddDate(0, 0, 1), "got", fs.Date)
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
	svc, err := job.NewJobService(tk, start, "fake-bucket", sources, &NullSaver{})
	must(t, err)

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

		// TODO - this should be pulled into a separate test
		if k == 2 {
			job := tracker.Job{}
			json.Unmarshal([]byte(expected[0].body), &job)
			status, _ := tk.GetStatus(job)
			status.NewState(tracker.Complete)
			err := tk.UpdateJob(job, status)
			if err != nil {
				t.Error(err)
			}
		}
	}
}
