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
	q, err := dedup.NewQueryParams(job, "mlab-sandbox")
	rtx.Must(err, "dedup.Query failed")
	qs := q.QueryString("dedup")
	if !strings.Contains(qs, "uuid") {
		t.Error("query should contain keep.uuid:\n", q)
	}
	if !strings.Contains(qs, `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04":\n`, q)
	}
	if !strings.Contains(qs, "ParseInfo.TaskFileName") {
		t.Error("query should contain ParseInfo.TaskFileName:\n", q)
	}
	// TODO check final WHERE clause.
	if !strings.Contains(qs, "target.ParseInfo.ParseTime = keep.ParseTime AND") {
		t.Error("query should contain target.ParseInfo.ParseTime = ... :\n", q)
	}
}

// NOTE: This validates queries against actual tables in mlab-sandbox.  It only
// runs Dryrun queries, so it does not modify the tables.
func TestValidateQueries(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping test for --short")
	}
	ctx := context.Background()
	dataTypes := []string{"tcpinfo", "ndt5", "annotation", "ndt7"}
	keys := []string{"dedup", "cleanup"} // TODO Add "preserve" query
	for _, dataType := range dataTypes {
		job := tracker.NewJob("bucket", "ndt", dataType, time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
		qp, err := dedup.NewQueryParams(job, "mlab-sandbox")
		if err != nil {
			t.Fatal(dataType, err)
		}
		for _, key := range keys {

			t.Run(dataType+":"+key, func(t *testing.T) {
				j, err := qp.Run(ctx, key, true)
				if err != nil {
					t.Fatal(t.Name(), err, qp.QueryString(key))
				}
				status := j.LastStatus()
				if status.Err() != nil {
					t.Fatal(t.Name(), err, qp.QueryString(key))
				}
			})
		}
		t.Run(dataType+":copy", func(t *testing.T) {
			j, err := qp.Copy(ctx, true)
			if err != nil {
				t.Fatal(t.Name(), err)
			}
			status := j.LastStatus()
			if status.Err() != nil {
				t.Fatal(t.Name(), err)
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
		t.Fatal(err, qp.QueryString("dedup"))
	}
	t.Error(status)
}

func TestQueryParams_String(t *testing.T) {
}
