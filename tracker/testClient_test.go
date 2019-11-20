package tracker_test

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/datastore/dsiface"
)

func validateDatastoreEntity(e interface{}) error {
	v := reflect.ValueOf(e)
	if v.Kind() != reflect.Ptr {
		return datastore.ErrInvalidEntityType
	}
	// NOTE: This is over-restrictive, but fine for current purposes.
	if reflect.Indirect(v).Kind() != reflect.Struct {
		return datastore.ErrInvalidEntityType
	}
	return nil
}

var ErrNotImplemented = errors.New("Not implemented")

// This implements a crude datastore test client.  It is somewhat
// simplistic and incomplete.  It works only for basic Put, Get, and Delete,
// but may not work always work correctly.
type testClient struct {
	dsiface.Client // For unimplemented methods
	lock           sync.Mutex
	objects        map[datastore.Key]reflect.Value
}

func newTestClient() *testClient {
	return &testClient{objects: make(map[datastore.Key]reflect.Value, 10)}
}

func (c *testClient) Close() error { return nil }

func (c *testClient) Count(ctx context.Context, q *datastore.Query) (n int, err error) {
	return 0, ErrNotImplemented
}

func (c *testClient) Delete(ctx context.Context, key *datastore.Key) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.objects[*key]
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	delete(c.objects, *key)
	return nil
}

func (c *testClient) Get(ctx context.Context, key *datastore.Key, dst interface{}) (err error) {
	err = validateDatastoreEntity(dst)
	if err != nil {
		return err
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	v := reflect.ValueOf(dst)
	o, ok := c.objects[*key]
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	v.Elem().Set(o)
	return nil
}

func (c *testClient) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	err := validateDatastoreEntity(src)
	if err != nil {
		return nil, err
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	v := reflect.ValueOf(src)
	c.objects[*key] = reflect.Indirect(v)
	return key, nil
}
