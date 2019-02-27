package tq_test

import (
	"log"
	"testing"

	"github.com/m-lab/go/prometheusx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/tq"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestPostOneTask(t *testing.T) {
	client, counter := cloud.DryRunClient()
	config := cloud.Config{Client: client, Project: "mlab-testing"}
	q, err := tq.NewQueueHandler(config, "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	q.PostOneTask("archive-mlab-testing", "test-file")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Count() != 1 {
		t.Error("Should have count of 1")
	}
}

func TestMetrics(t *testing.T) {
	tq.EmptyStatsRecoveryTimeHistogramSecs.WithLabelValues("x")
	prometheusx.LintMetrics(t)
}
