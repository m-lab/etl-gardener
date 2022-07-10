// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/go-test/deep"

	"github.com/m-lab/go/testingx"

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

	ds := config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: ds},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: ds},
	}
	// This is three days before "now".  The job service should restart
	// when it reaches 36 hours before "now", which is 2011-02-05
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	svc, err := job.NewJobService(ctx, &NullTracker{}, start, "fake-bucket", sources, &NullSaver{}, nil)
	must(t, err)

	expected := []struct {
		body string
	}{
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-03T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-04T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}`},
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-04T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}`},
		// Wrap
		{body: `{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-03T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}`},
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

func TestResume(t *testing.T) {
	// Fake time will avoid yesterday trigger.
	now := time.Date(2011, 2, 16, 1, 2, 3, 4, time.UTC)
	monkey.Patch(time.Now, func() time.Time {
		return now
	})
	defer monkey.Unpatch(time.Now)

	ctx := context.Background()

	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	saver := tracker.NewLocalSaver(t.TempDir(), nil)
	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}
	lastJobDate := start.AddDate(0, 0, 3)
	last := tracker.NewJob("fake-bucket", "ndt", "ndt5", lastJobDate)
	tk.AddJob(last)

	ds := config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: ds},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: ds},
	}
	svc, err := job.NewJobService(ctx, tk, start, "fake-bucket", sources, &NullSaver{}, nil)
	must(t, err)
	j := svc.NextJob(ctx)
	if j.Job.Date != last.Date {
		t.Error(j, last)
	}
}

// Implements persistence.Saver, for test injection.
type FakeSaver struct {
	Current   time.Time
	Yesterday time.Time
}

func (fs *FakeSaver) Save(ctx context.Context, o persistence.StateObject) error {
	switch svc := o.(type) {
	case *job.Service:
		fs.Current = svc.Date
	case *job.YesterdaySource:
		fs.Yesterday = svc.Date
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
		to.Date = fs.Current
	case *job.YesterdaySource:
		to.Date = fs.Yesterday
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

	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 16, 1, 2, 3, 4, time.UTC)
	})

	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	ds := config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: ds},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: ds},
	}

	// Set up fake saver.
	resume := time.Date(2011, 2, 10, 0, 0, 0, 0, time.UTC)
	// yesterday is set to now, so it won't trigger.
	yesterday := time.Now().UTC().Truncate(24 * time.Hour)
	fs := FakeSaver{Current: resume, Yesterday: yesterday}
	svc, err := job.NewJobService(ctx, &NullTracker{}, start, "fake-bucket", sources, &fs, nil)
	must(t, err)
	// NextJob should return a job with date provided by FakeSaver.
	j := svc.NextJob(ctx)
	if j.Job.Date != resume {
		t.Error("Expected ", resume, "got", j)
	}

	// When NextJob returns the last job for the date, it should trigger
	// saving the next date to process, which is 20110214.
	j = svc.NextJob(ctx)
	// Check that we see the new date in the FakeSaver
	if fs.Current != resume.AddDate(0, 0, 1) {
		t.Error("Expected", resume.AddDate(0, 0, 1), "got", fs.Current)
	}
}

func TestYesterdayFromSaver(t *testing.T) {
	ctx := context.Background()

	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		// NOTE: must be later than the default "yesterday" delay.
		return time.Date(2011, 2, 16, 11, 2, 3, 4, time.UTC)
	})

	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	ds := config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}
	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: ds},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: ds},
	}

	// Set up fake saver.
	resume := time.Date(2011, 2, 10, 0, 0, 0, 0, time.UTC)
	// Set up yesterday so that it triggers immediately.
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)
	fs := FakeSaver{Current: resume, Yesterday: yesterday}
	svc, err := job.NewJobService(ctx, &NullTracker{}, start, "fake-bucket", sources, &fs, nil)
	must(t, err)

	expected := []struct {
		body string
	}{
		// Yesterday (twice to catch up)
		{body: `{"ID":"fake-bucket/ndt/ndt5/20110214","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-14T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/tcpinfo/20110214","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-14T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/ndt5/20110215","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-15T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/tcpinfo/20110215","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-15T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		// Resume
		{body: `{"ID":"fake-bucket/ndt/ndt5/20110210","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-10T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/tcpinfo/20110210","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-10T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/ndt5/20110211","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"ndt5","Date":"2011-02-11T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
		{body: `{"ID":"fake-bucket/ndt/tcpinfo/20110211","Job":{"Bucket":"fake-bucket","Experiment":"ndt","Datatype":"tcpinfo","Date":"2011-02-11T00:00:00Z","Datasets":{"Temp":"tmp_ndt","Raw":"raw_ndt","Join":"ndt"}}}`},
	}

	for i, e := range expected {
		want := &tracker.JobWithTarget{}
		json.Unmarshal([]byte(e.body), want)
		got := svc.NextJob(ctx)
		diff := deep.Equal(want, got)
		if diff != nil {
			t.Error(i, diff)
		}
	}

	// Check that we see the new date in the FakeSaver
	if fs.Yesterday != yesterday.AddDate(0, 0, 2) {
		t.Error("Expected", yesterday.AddDate(0, 0, 1), "got", fs.Yesterday)
	}
}

func TestEarlyWrapping(t *testing.T) {
	ctx := context.Background()

	// This allows predictable behavior from time.Since in the advanceDate function.
	monkey.Patch(time.Now, func() time.Time {
		return time.Date(2011, 2, 6, 1, 2, 3, 4, time.UTC)
	})
	defer monkey.Unpatch(time.Now)
	start := time.Date(2011, 2, 3, 0, 0, 0, 0, time.UTC)
	saver := tracker.NewLocalSaver(t.TempDir(), nil)
	tk, err := tracker.InitTracker(context.Background(), saver, 0, 0, 0) // Only using jobmap.
	if err != nil {
		t.Fatal(err)
	}

	sources := []config.SourceConfig{
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}},
		{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "pcap", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"}, DailyOnly: true}, // skipped.
	}
	svc, err := job.NewJobService(ctx, tk, start, "fake-bucket", sources, &NullSaver{}, nil)
	must(t, err)

	// If a job is still present in the tracker when it wraps, /job returns an error.
	expected := []tracker.Job{
		tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}, Date: start},
		tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}, Date: start},
		tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}, Date: start.AddDate(0, 0, 1)},
		tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}, Date: start.AddDate(0, 0, 1)},
		// This one should work, because we complete it in the loop.
		tracker.Job{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Datasets: config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"}, Date: start},
	}

	for k, realJob := range expected {
		got := svc.NextJob(context.Background())
		tk.AddJob(got.Job)
		if k == 2 {
			// Simulate "monitor" behavior and mark job complete to prevent AddJob error.
			status, _ := tk.GetStatus(got.Job.Key())
			status.NewState(tracker.Complete)
			err := tk.UpdateJob(got.Job.Key(), status)
			if err != nil {
				t.Error(err)
			}
		}

		if got.Job != realJob {
			t.Errorf("NextJob() wrong job: got %v, want %v", got.Job, realJob)
		}
	}
}

func assertYesterdayStateObject(so persistence.StateObject) {
	assertYesterdayStateObject(&job.YesterdaySource{})
}

func TestPersistence(t *testing.T) {
	ctx := context.Background()
	ls := persistence.NewLocalSaver(t.TempDir())

	now := time.Now()
	svc := job.Service{Date: now}
	err := ls.Save(ctx, &svc)
	testingx.Must(t, err, "Save error")

	svc.Date = time.Time{}
	err = ls.Fetch(ctx, &svc)
	testingx.Must(t, err, "Fetch error")
	if svc.Date.Unix() != now.Unix() {
		t.Error("Date should be now", &svc, now.Unix(), svc.Date.Unix())
	}

	err = ls.Delete(ctx, &svc)
	testingx.Must(t, err, "Delete error")
	err = ls.Fetch(ctx, &svc)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Should have errored", err)
	}
}
