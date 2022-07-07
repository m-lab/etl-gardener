package config_test

import (
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/config"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		start   string
		want    *config.Gardener
		wantErr bool
	}{
		{
			name:  "success",
			file:  "testdata/config.yml",
			start: "2019-03-04",
			want: &config.Gardener{
				StartDate: time.Date(2019, time.March, 4, 0, 1, 2, 0, time.UTC),
				Tracker:   config.TrackerConfig{Timeout: 5 * time.Hour},
				Monitor:   config.MonitorConfig{PollingInterval: 5 * time.Minute},
				Sources: []config.SourceConfig{
					{Bucket: "archive-measurement-lab", Experiment: "ndt", Datatype: "tcpinfo", Filter: ".*T??:??:00.*Z", Target: "ndt.tcpinfo", DailyOnly: false},
					{Bucket: "archive-measurement-lab", Experiment: "ndt", Datatype: "ndt5", Filter: ".*T??:??:00.*Z", Target: "ndt.ndt5", DailyOnly: false},
					{Bucket: "archive-measurement-lab", Experiment: "ndt", Datatype: "pcap", Filter: "", Target: "ndt.pcap", DailyOnly: true},
				},
			},
		},
		{
			name:    "error-no-file",
			file:    "",
			wantErr: true,
		},
		{
			name:    "error-no-such-file",
			file:    "this-file-does-not-exist",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.ParseConfig(tt.file)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.want)
			}
			if got.Start().Format("2006-01-02") != tt.start {
				t.Errorf("Gardener.Start() wrong date; got %q, want %q", got.Start().Format("2006-01-02"), tt.start)
			}
		})
	}
}
