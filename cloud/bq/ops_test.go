package bq_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func TestTemplate(t *testing.T) {
	job := tracker.NewJob("bucket", "ndt", "annotation", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q, err := bq.NewQuerier(context.Background(), job, "fake-project", "")
	rtx.Must(err, "dedup.Query failed")
	qs := q.QueryFor("dedup")
	if !strings.Contains(qs, "keep.id") {
		t.Error("query should contain keep.uuid:\n", q)
	}
	if !strings.Contains(qs, `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04":\n`, q)
	}
	// TODO check final WHERE clause.
	if !strings.Contains(qs, "target.parser.Time = keep.Time") {
		t.Error("query should contain target.parser.Time = ... :\n", qs)
	}
}

// NOTE: This validates queries against actual tables in mlab-testing.  It only
// runs Dryrun queries, so it does not modify the tables.
func TestValidateQueries(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping test for --short")
	}
	ctx := context.Background()
	dataTypes := []string{"annotation", "ndt7"}
	keys := []string{"dedup"} // TODO Add "preserve" query
	// Test for each datatype
	for _, dataType := range dataTypes {
		job := tracker.NewJob("bucket", "ndt", dataType, time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
		qp, err := bq.NewQuerier(ctx, job, "mlab-testing", "")
		if err != nil {
			t.Fatal(dataType, err)
		}
		// Test each query key
		for _, key := range keys {
			t.Run(dataType+":"+key, func(t *testing.T) {
				t.Log(t.Name())
				j, err := qp.Run(ctx, key, true)
				if err != nil {
					t.Fatal(t.Name(), err, qp.QueryFor(key))
				}
				status := j.LastStatus()
				if status.Err() != nil {
					t.Fatal(t.Name(), err, qp.QueryFor(key))
				}
			})
		}
		//		t.Run(dataType+":copy", func(t *testing.T) {
		//			j, err := qp.Copy(ctx, true)
		//			if err != nil {
		//				t.Fatal(t.Name(), err)
		//			}
		//			status := j.LastStatus()
		//			if status.Err() != nil {
		//				t.Fatal(t.Name(), status.Err())
		//			}
		//		})
	}
}
