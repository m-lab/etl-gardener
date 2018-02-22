// +build integration

package tq_test

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"google.golang.org/api/iterator"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestGetTaskqueueStats(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	// TODO - use mlab-testing instead of mlab-sandbox??
	//bucket, err := tq.GetBucket(MLabTestAuth(), "mlab-sandbox", "archive-mlab-test", true)

	q, err := tq.CreateQueuer(http.DefaultClient, MLabTestAuth(), "test-", 8, "mlab-sandbox", "archive-mlab-test", true)
	stats, err := q.GetTaskqueueStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 8 {
		t.Fatal("Too few stats")
	}
	log.Println(stats)
}

// NOTE: this test depends on actual bucket content.  If it starts failing,
// check that the bucket content has not been changed.
func TestGetBucket(t *testing.T) {
	bucketName := "archive-mlab-test"
	bucket, err := tq.GetBucket(MLabTestAuth(), "mlab-testing", bucketName, false)
	if err != nil {
		t.Fatal(err)
	}

	prefix := "ndt/2017/09/24/"
	qry := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - can this error?  Or do errors only occur on iterator ops?
	it := bucket.Objects(context.Background(), &qry)
	count := 0
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			log.Println(err)
			t.Fatal(err)
		}
		if !strings.Contains(o.Name, prefix) {
			t.Error("wrong prefix " + o.Name)
		}
		count++
	}
	if count != 76 {
		t.Error("Wrong number of objects: ", count)
	}
}

// NOTE: this test depends on actual bucket content.  If it starts failing,
// check that the bucket content has not been changed.
func TestPostDay(t *testing.T) {
	client, counter := tq.DryRunQueuerClient()
	q, err := tq.CreateQueuer(client, MLabTestAuth(), "test-", 8, "fake-project", "archive-mlab-test", true)
	if err != nil {
		t.Fatal(err)
	}
	bucketName := "archive-mlab-test"
	bucket, err := tq.GetBucket(MLabTestAuth(), "mlab-testing", bucketName, false)
	q.PostDay(nil, bucket, bucketName, "ndt/2017/09/24/")
	if counter.Count() != 76 {
		t.Errorf("Should have made 76 http requests: %d\n", counter.Count())
	}
}
