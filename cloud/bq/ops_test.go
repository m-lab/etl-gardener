//go:build integration
// +build integration

package bq_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func TestTemplate(t *testing.T) {
	job := jobtest.NewJob("bucket", "ndt", "annotation", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q, err := bq.NewTableOps(context.Background(), job, "fake-project", "")
	rtx.Must(err, "NewTableOps failed")
	qs := bq.DedupQuery(*q)
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
	d := time.Date(2020, 9, 5, 0, 0, 0, 0, time.UTC).UTC().Truncate(24 * time.Hour)
	jobs := []tracker.Job{
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "annotation",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "ndt7",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "pcap",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "hopannotation1",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt"},
		},
		{
			Bucket:     "bucket",
			Experiment: "ndt",
			Datatype:   "scamper1",
			Date:       d,
			Datasets:   config.Datasets{Temp: "tmp_ndt", Raw: "raw_ndt", Join: "ndt"},
		},
	}

	// Test for each datatype
	for _, job := range jobs {
		qp, err := bq.NewTableOps(ctx, job, "mlab-testing", "")
		if err != nil {
			t.Fatal(job.Datatype, err)
		}
		t.Run(job.Datatype+":dedup", func(t *testing.T) {
			t.Log(t.Name())
			j, err := qp.Dedup(ctx, true)
			if err != nil {
				t.Fatal(t.Name(), err, bq.DedupQuery(*qp))
			}
			status := j.LastStatus()
			if status.Err() != nil {
				t.Fatal(t.Name(), err, bq.DedupQuery(*qp))
			}

			if qp.Job.Datasets.Join == "" {
				return
			}

			j, err = qp.Join(ctx, true)
			if err != nil {
				t.Fatal(t.Name(), err, bq.JoinQuery(*qp))
			}
			status = j.LastStatus()
			if status.Err() != nil {
				t.Fatal(t.Name(), err, bq.JoinQuery(*qp))
			}
		})
	}
}
