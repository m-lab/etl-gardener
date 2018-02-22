package tq_test

import (
	"log"
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

func TestPostOneTask(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	// TODO - use mlab-testing instead of mlab-sandbox??
	client, counter := tq.DryRunQueuerClient()
	q, err := tq.CreateQueuer(client, Options(), "test-", 8, "mlab-sandbox", "archive-mlab-test", true)
	q.PostOneTask("test-queue", "archive-mlab-test", "test-file")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Count() != 1 {
		t.Error("Should have count of 1")
	}
}
