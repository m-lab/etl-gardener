// Package job provides an http handler to serve up jobs to ETL parsers.
package job_test

import (
	"context"
	"errors"
	"path"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/gcsfake"
)

type namedSaver interface {
	Save(v any) error
	Load(v any) error
}

func TestNewJobService(t *testing.T) {
	tests := []struct {
		name        string
		startDate   time.Time
		sources     []config.SourceConfig
		statsClient stiface.Client
		dailySaver  namedSaver
		histSaver   namedSaver
		want        tracker.Key
		wantErr     error
	}{
		{
			name:      "successful-init",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", FullHistory: true},
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", FullHistory: true},
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "pcap", DailyOnly: true},
			},
			dailySaver: &failSaver{err: errors.New("any error")},
			histSaver:  &noopSaver{},
			want:       "fake-bucket/ndt/ndt5/20220701",
		},
		{
			name:      "error-bad-start-time",
			startDate: time.Time{},
			wantErr:   job.ErrInvalidStartDate,
		},
		{
			name:      "error-no-configured-jobs",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources:   []config.SourceConfig{},
			wantErr:   job.ErrNoConfiguredJobs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := job.NewJobService(tt.startDate, tt.sources, tt.statsClient, tt.dailySaver, tt.histSaver)
			if tt.wantErr != err {
				t.Errorf("NewJobService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr != nil {
				return
			}
			// Attempt to read one job.
			jt := got.NextJob(context.Background())
			if jt == nil {
				t.Errorf("NewJobService().NextJob nil job; got nil, want job")
				return
			}
			if jt.ID != tt.want {
				t.Errorf("NewJobService().NextJob wrong ID; got %q, want %q", jt.ID, tt.want)
			}
		})
	}
}

func TestService_NextJob(t *testing.T) {
	// Setup fake gcs bucket access.
	fakeb := gcsfake.NewBucketHandle()
	fakeb.ObjAttrs = append(fakeb.ObjAttrs, &storage.ObjectAttrs{Name: "ndt/ndt5/2022/07/01/foo.tgz", Size: 1, Updated: time.Now()})
	fakec := &gcsfake.GCSClient{}
	fakec.AddTestBucket("fake-bucket", fakeb)

	// Setup fake daily checkpoint.
	faked := &job.DailyIterator{Date: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC)}
	ps := persistence.NewLocalNamedSaver(path.Join(t.TempDir(), "fake-daily.json"))
	ps.Save(faked)

	tests := []struct {
		name        string
		startDate   time.Time
		sources     []config.SourceConfig
		statsClient stiface.Client
		dailySaver  namedSaver
		histSaver   namedSaver
		want        tracker.Key
		wantErr     bool
		cancelCtx   bool
	}{
		{
			name:      "success-historical",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5",
					FullHistory: true},
			},
			statsClient: fakec,
			dailySaver:  &failSaver{err: errors.New("any error")},
			histSaver:   &noopSaver{},
			want:        "fake-bucket/ndt/ndt5/20220701",
		},
		{
			name:      "success-daily",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "pcap", DailyOnly: true},
			},
			dailySaver: ps, // use persisted and restored date.
			histSaver:  &noopSaver{},
			want:       "fake-bucket/ndt/pcap/20220701",
		},
		{
			name:      "error-fail-savers",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5",
					FullHistory: true},
			},
			dailySaver: &failSaver{err: errors.New("fail")},
			histSaver:  &failSaver{err: errors.New("fail")},
			wantErr:    true,
		},
		{
			name:      "error-listing-pcap",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "pcap"},
			},
			statsClient: fakec, // there are no pcap files in fakec client.
			dailySaver:  &noopSaver{},
			histSaver:   &noopSaver{},
			wantErr:     true,
		},
		{
			name:      "error-cancelled-context",
			startDate: time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC),
			sources: []config.SourceConfig{
				{Bucket: "fake-bucket", Experiment: "ndt", Datatype: "pcap"},
			},
			statsClient: fakec, // HasFiles will fail b/c the context will be cancelled.
			dailySaver:  &noopSaver{},
			histSaver:   &noopSaver{},
			wantErr:     true,
			cancelCtx:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := job.NewJobService(tt.startDate, tt.sources, tt.statsClient, tt.dailySaver, tt.histSaver)
			if err != nil {
				t.Errorf("NewJobService() error = %v, want nil", err)
				return
			}
			ctx := context.Background()
			if tt.cancelCtx {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)
				cancel() // cancel it.
			}
			got := svc.NextJob(ctx)
			if (got == nil) != tt.wantErr {
				t.Errorf("NextJob() returned %v, wantErr %t", got, tt.wantErr)
				return
			}
			if got == nil {
				return
			}
			if got.ID != tt.want {
				t.Errorf("Service.NextJob() wrong job; got %v, want %v", got.ID, tt.want)
			}
		})
	}
}
