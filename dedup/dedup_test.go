package dedup_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/dedup"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func TestTemplate(t *testing.T) {
	job := tracker.NewJob("bucket", "ndt", "tcpinfo", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q, err := dedup.Query(job, "mlab-sandbox")
	rtx.Must(err, "dedup.Query failed")
	if !strings.Contains(q.String(), "uuid") {
		t.Error("query should contain keep.uuid:\n", q)
	}
	if !strings.Contains(q.String(), `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04":\n`, q)
	}
	if !strings.Contains(q.String(), "ParseInfo.TaskFileName") {
		t.Error("query should contain ParseInfo.TaskFileName:\n", q)
	}
	// TODO check final WHERE clause.
	if !strings.Contains(q.String(), "target.ParseInfo.ParseTime = keep.ParseTime AND") {
		t.Error("query should contain target.ParseInfo.ParseTime = ... :\n", q)
	}
}

func TestValidateQueries(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping test for --short")
	}
	tests := []struct {
		name string
		job  tracker.Job
	}{
		{
			name: "tcpinfo",
			job:  tracker.NewJob("bucket", "ndt", "tcpinfo", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC)),
		},
		{
			name: "ndt5",
			job:  tracker.NewJob("bucket", "ndt", "ndt5", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := dedup.Query(tt.job, "mlab-testing")
			if err != nil {
				t.Fatal(tt.name, err)
			}
			j, err := q.Dedup(context.Background(), true)
			if err != nil {
				t.Fatal(tt.name, err)
			}
			status := j.LastStatus()
			if status.Err() != nil {
				t.Fatal(tt.name, err)
			}
		})
	}
}

// This runs a real dedup on a real partition in mlab-sandbox for manual testing.
// It takes about 5 minutes to run.
func xTestDedup(t *testing.T) {
	job := tracker.NewJob("foobar", "ndt", "tcpinfo", time.Date(2019, 8, 12, 0, 0, 0, 0, time.UTC))
	qp := dedup.QueryParams{
		Project:   "mlab-sandbox",
		TestTime:  "TestTime",
		Job:       job,
		Partition: map[string]string{"uuid": "uuid", "ParseTime": "ParseInfo.ParseTime"},
		Order:     "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC",
		Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
	}
	bqjob, err := qp.Dedup(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	status, err := bqjob.Wait(context.Background())
	if err != nil {
		t.Fatal(err, qp.String())
	}
	t.Error(status)
}

func TestQueryParams_String(t *testing.T) {
}
