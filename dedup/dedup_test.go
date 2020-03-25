package dedup

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestTemplate(t *testing.T) {
	job := tracker.NewJob("bucket", "ndt", "tcpinfo", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q := TCPInfoQuery(job, "mlab-sandbox").String()
	if !strings.Contains(q, "uuid") {
		t.Error("query should contain keep.uuid:\n", q)
	}
	if !strings.Contains(q, `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04":\n`, q)
	}
	if !strings.Contains(q, "ParseInfo.TaskFileName") {
		t.Error("query should contain ParseInfo.TaskFileName:\n", q)
	}
	// TODO check final WHERE clause.
	if !strings.Contains(q, "target.ParseInfo.ParseTime = keep.ParseTime AND") {
		t.Error("query should contain target.ParseInfo.ParseTime = ... :\n", q)
	}
	t.Error(q)
}

// This runs a real dedup on a real partition in mlab-sandbox for manual testing.
// It takes about 5 minutes to run.
func xTestDedup(t *testing.T) {
	job := tracker.NewJob("foobar", "ndt", "tcpinfo", time.Date(2019, 8, 12, 0, 0, 0, 0, time.UTC))
	qp := QueryParams{
		Project:   "mlab-sandbox",
		TestTime:  "TestTime",
		Job:       job,
		Partition: map[string]string{"uuid": "uuid", "ParseTime": "ParseInfo.ParseTime"},
		Order:     "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC",
		Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
	}
	bqjob, err := qp.Dedup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, err := bqjob.Wait(context.Background())
	if err != nil {
		t.Fatal(err, qp.String())
	}
	t.Error(status)
}
