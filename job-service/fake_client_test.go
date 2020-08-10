package job_test

import (
	"context"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"
)

// By using the "interface" version of the client, we make it possible to sub in
// our own fakes at any level. Here we sub in a fake Client which returns a fake
// BucketHandle that returns a series of fake Objects.
type fakeClient struct {
	stiface.Client
	objects []*storage.ObjectAttrs // Objects that will be returned by iterator
}

func (f fakeClient) Close() error {
	return nil
}

func (f fakeClient) Bucket(name string) stiface.BucketHandle {
	return &fakeBucketHandle{objects: f.objects}
}

type fakeBucketHandle struct {
	stiface.BucketHandle
	objects []*storage.ObjectAttrs // Objects that will be returned by iterator
}

func (bh fakeBucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{}, nil
}

func (bh fakeBucketHandle) Objects(ctx context.Context, q *storage.Query) stiface.ObjectIterator {
	n := 0
	return fakeObjectIterator{next: &n, objects: bh.objects, q: q}
}

type fakeObjectIterator struct {
	stiface.ObjectIterator
	objects []*storage.ObjectAttrs
	q       *storage.Query
	next    *int
}

func (it fakeObjectIterator) Next() (*storage.ObjectAttrs, error) {
	for *it.next < len(it.objects) {
		next := it.objects[*it.next]
		*it.next += 1
		// TODO should also check if there are more delimiters.
		if it.q == nil || it.q.Prefix == "" || strings.HasPrefix(next.Name, it.q.Prefix) {
			return next, nil
		}
	}
	return nil, iterator.Done
}
