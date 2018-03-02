// +build integration

package tq_test

import (
	"context"
	"log"
	"net/http"
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
	stats, err := tq.GetTaskqueueStats("mlab-sandbox", "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	log.Println(stats)
}

// NOTE: this test depends on actual bucket content.  If it starts failing,
// check that the bucket content has not been changed.
func TestGetBucket(t *testing.T) {
	bucketName := "archive-mlab-test"
	bucket, err := tq.GetBucket(nil, "mlab-testing", bucketName, false)
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

func TestIsEmpty(t *testing.T) {
	q, err := tq.NewQueueHandler(http.DefaultClient, "mlab-sandbox", "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	err = q.IsEmpty()
	if err != nil && err != tq.ErrMoreTasks {
		t.Fatal(err)
	}
}

// NOTE: this test depends on actual bucket content.  If it starts failing,
// check that the bucket content has not been changed.
func TestPostDay(t *testing.T) {
	// Use a fake queue client.
	client, counter := tq.DryRunQueuerClient()
	q, err := tq.NewQueueHandler(client, "fake-project", "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	// Use a real storage bucket.
	bucketName := "archive-mlab-test"
	bucket, err := tq.GetBucket(nil, "mlab-testing", bucketName, false)
	if err != nil {
		t.Fatal(err)
	}
	q.PostDay(bucket, bucketName, "ndt/2017/09/24/")
	if counter.Count() != 76 {
		t.Errorf("Should have made 76 http requests: %d\n", counter.Count())
	}
}
