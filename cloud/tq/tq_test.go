package tq_test

import (
	"log"
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/tq"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestPostOneTask(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	client, counter := tq.DryRunQueuerClient()
	q, err := tq.NewQueueHandler(client, "mlab-testing", "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	q.PostOneTask("archive-mlab-test", "test-file")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Count() != 1 {
		t.Error("Should have count of 1")
	}
}
