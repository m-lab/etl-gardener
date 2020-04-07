package ops_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/dataset"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/option"
)

func TestTableUtils(t *testing.T) {
	ctx := context.Background()

	project := "fakeProject"
	dryRun, _ := cloud.DryRunClient()
	config := cloud.Config{
		Project: project,
		Client:  nil,
		Options: []option.ClientOption{option.WithHTTPClient(dryRun)},
	}
	bqConfig := cloud.BQConfig{Config: config, BQFinalDataset: "final", BQBatchDataset: "batch"}

	c, err := bigquery.NewClient(ctx, bqConfig.BQProject, bqConfig.Options...)
	rtx.Must(err, "client")
	bqClient := bqiface.AdaptClient(c)
	ds := dataset.Dataset{Dataset: bqClient.Dataset(bqConfig.BQBatchDataset), BqClient: bqClient}

	j := tracker.NewJob(
		"bucket", "exp", "type", time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC))
	src := ops.TemplateTable(j, &ds)
	dest := ops.PartitionedTable(j, &ds)

	if src.DatasetID() != "batch" {
		t.Error(src)
	}
	if dest.DatasetID() != "batch" {
		t.Error(src)
	}

}

func TestStandardMonitor(t *testing.T) {
	logx.LogxDebug.Set("true")

	ctx, cancel := context.WithCancel(context.Background())
	tk, err := tracker.InitTracker(ctx, nil, nil, 0, 0, 0)
	rtx.Must(err, "tk init")
	tk.AddJob(tracker.NewJob("bucket", "exp", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type", time.Now()))
	tk.AddJob(tracker.NewJob("bucket", "exp2", "type2", time.Now()))

	m, err := ops.NewStandardMonitor(context.Background(), cloud.BQConfig{}, tk)
	rtx.Must(err, "NewMonitor failure")
	// We add some new actions in place of the Parser activity.
	m.AddAction(tracker.Init,
		nil,
		newStateFunc(tracker.Parsing),
		"Init")
	m.AddAction(tracker.Parsing,
		nil,
		newStateFunc(tracker.ParseComplete),
		"Parsing")

	// Substitute, since we don't want to use real bq backend.
	m.AddAction(tracker.Stabilizing,
		nil,
		newStateFunc(tracker.Deduplicating),
		"Checking for stability")
	go m.Watch(ctx, 10*time.Millisecond)

	failTime := time.Now().Add(10 * time.Second)

	for time.Now().Before(failTime) && tk.NumFailed() < 3 {
		time.Sleep(time.Millisecond)
	}
	if tk.NumFailed() != 3 {
		t.Error(tk.NumFailed())
	}
	cancel()
}
