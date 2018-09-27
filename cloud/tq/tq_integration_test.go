// +build integration

package tq_test

import (
	"context"
	"log"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"google.golang.org/api/iterator"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestGetTaskqueueStats(t *testing.T) {
	config := cloud.Config{Client: http.DefaultClient, Project: "mlab-sandbox"}
	stats, err := tq.GetTaskqueueStats(config, "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	log.Println(stats)
}

// NOTE: this test depends on actual bucket content.  If it starts failing,
// check that the bucket content has not been changed.
// TODO - this currently leaks goroutines.
func TestGetBucket(t *testing.T) {
	ctx := context.Background()

	storageClient, err := storage.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	bucketName := "archive-mlab-testing"
	bucket, err := tq.GetBucket(ctx, storageClient, "mlab-testing", bucketName, false)
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
	if count != 3 {
		t.Error("Wrong number of objects: ", count)
	}

	storageClient.Close()
}

func TestIsEmpty(t *testing.T) {
	config := cloud.Config{Client: http.DefaultClient, Project: "mlab-sandbox"}
	q, err := tq.NewQueueHandler(config, "test-queue")
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
	client, counter := cloud.DryRunClient()
	ctx := context.Background()
	config := cloud.Config{Context: ctx, Client: client, Project: "fake-project"}
	q, err := tq.NewQueueHandler(config, "test-queue")
	if err != nil {
		t.Fatal(err)
	}

	// Use a real storage bucket.
	storageClient, err := storage.NewClient(context.Background())
	if err != nil {
		t.Error(err)
	}
	bucketName := "archive-mlab-testing"
	bucket, err := tq.GetBucket(config.Context, storageClient, "mlab-testing", bucketName, false)
	if err != nil {
		t.Fatal(err)
	}
	n, err := q.PostDay(bucket, bucketName, "ndt/2017/09/24/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Error("Should have posted 3 items", n)
	}
	n, err = q.PostDay(bucket, bucketName, "ndt/2018/05/01/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 45 {
		t.Error("Should have posted 45 items", n)
	}
	if counter.Count() != 48 {
		t.Error("Should have made 48 http requests:", counter.Count())
	}
	storageClient.Close()
}
