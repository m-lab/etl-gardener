package jobtest

import (
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestNewJob(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		exp    string
		typ    string
		date   time.Time
		want   tracker.Job
	}{
		{
			name:   "success",
			bucket: "bucket",
			exp:    "exp",
			typ:    "typ",
			date:   time.Date(2019, time.February, 03, 00, 00, 00, 00, time.UTC),
			want: tracker.Job{
				Bucket:     "bucket",
				Experiment: "exp",
				Datatype:   "typ",
				Date:       time.Date(2019, time.February, 03, 00, 00, 00, 00, time.UTC),
				Datasets: tracker.Datasets{
					Tmp: "tmp_exp",
					Raw: "raw_exp",
				},
			},
		},
		{
			name:   "success-trucate-date",
			bucket: "bucket",
			exp:    "exp",
			typ:    "typ",
			date:   time.Date(2019, time.February, 03, 02, 03, 04, 05, time.UTC),
			want: tracker.Job{
				Bucket:     "bucket",
				Experiment: "exp",
				Datatype:   "typ",
				Date:       time.Date(2019, time.February, 03, 00, 00, 00, 00, time.UTC),
				Datasets: tracker.Datasets{
					Tmp: "tmp_exp",
					Raw: "raw_exp",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewJob(tt.bucket, tt.exp, tt.typ, tt.date); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewJob() = %v, want %v", got, tt.want)
			}
		})
	}
}
