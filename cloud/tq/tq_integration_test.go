// +build integration

package tq_test

import (
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/tq"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestGetTaskqueueStats(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	// TODO - use mlab-testing instead of mlab-sandbox??
	//bucket, err := tq.GetBucket(Options(), "mlab-sandbox", "archive-mlab-test", true)

	q, err := tq.CreateQueuer(http.DefaultClient, Options(), "test-", 8, "mlab-sandbox", "archive-mlab-test", true)
	stats, err := q.GetTaskqueueStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 8 {
		t.Fatal("Too few stats")
	}
	log.Println(stats)
}
