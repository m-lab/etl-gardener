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
	q, err := tq.CreateQueuer(client, nil, "test-", 8, "mlab-testing", "archive-mlab-test", true)
	if err != nil {
		t.Fatal(err)
	}
	q.PostOneTask("test-queue", "archive-mlab-test", "test-file")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Count() != 1 {
		t.Error("Should have count of 1")
	}
}
