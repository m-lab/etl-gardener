package ops_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/dataset"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/option"
)

func TestTableUtils(t *testing.T) {
	ctx := context.Background()

	project := "fakeProject"
	dryRun, _ := cloud.DryRunClient()
	config := cloud.Config{
		Project: project,
		Client:  http.DefaultClient,
		Options: []option.ClientOption{option.WithHTTPClient(dryRun)},
	}
	bqConfig := cloud.BQConfig{Config: config, BQFinalDataset: "final", BQBatchDataset: "batch"}

	c, err := bigquery.NewClient(ctx, bqConfig.BQProject, bqConfig.Options...)
	rtx.Must(err, "client")
	bqClient := bqiface.AdaptClient(c)
	ds := dataset.Dataset{Dataset: bqClient.Dataset(bqConfig.BQBatchDataset), BqClient: bqClient}

	j := tracker.NewJob("bucket", "exp", "type", time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC))
	src := ops.TemplateTable(j, &ds)
	dest := ops.PartitionedTable(j, &ds)

	if src.DatasetID() != "batch" {
		t.Error(src)
	}
	if dest.DatasetID() != "batch" {
		t.Error(src)
	}

}
