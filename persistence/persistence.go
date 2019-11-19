package persistence

import (
	"context"
	"io/ioutil"
	"log"
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

var verbose = log.New(ioutil.Discard, "", 0)

func setVerbose(b bool) {
	if b {
		verbose = log.New(log.Writer(), log.Prefix(), log.Flags())
	} else {
		verbose = log.New(ioutil.Discard, "", 0)
	}
}

// StateObject defines the interface for objects to be saved/retrieved from datastore.
type StateObject interface {
	Key() string
	Kind() string // Should be implemented in the actual type, as return reflect.TypeOf(o).Name()
}

// Saver provides API for saving and retrieving StateObjects.
type Saver interface {
	Save(ctx context.Context, o StateObject) error
	Delete(ctx context.Context, o StateObject) error
	Fetch(ctx context.Context, o StateObject) error
}

// DatastoreSaver implements a Saver that stores state objects in Datastore.
type DatastoreSaver struct {
	Client    *datastore.Client
	Namespace string
}

// NewDatastoreSaver creates and returns an appropriate saver.
// ctx is only used to create the client.
// TODO - if this ever needs more context, use cloud.Config
func NewDatastoreSaver(ctx context.Context, project string) (*DatastoreSaver, error) {
	client, err := datastore.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	return &DatastoreSaver{Client: client, Namespace: "scoreboard"}, nil
}

func (ds *DatastoreSaver) key(o StateObject) *datastore.Key {
	k := datastore.NameKey(o.Kind(), o.Key(), nil)
	k.Namespace = ds.Namespace
	return k
}

// Save implements Saver.Save using Datastore.
func (ds *DatastoreSaver) Save(ctx context.Context, o StateObject) error {
	// These can be quite slow when there is a lot of activity.
	// 10 seconds seems sufficient.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	verbose.Println("Saving", o.Key())
	_, err := ds.Client.Put(ctx, ds.key(o), o)
	verbose.Println("Saved", o.Key(), err)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements Saver.Delete using Datastore.
func (ds *DatastoreSaver) Delete(ctx context.Context, o StateObject) error {
	// These can be quite slow when there is a lot of activity.
	// 10 seconds seems sufficient.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	verbose.Println("Deleting", o.Key())
	err := ds.Client.Delete(ctx, ds.key(o))
	verbose.Println("Deleted", o.Key(), err, time.Now())
	if err != nil {
		return err
	}
	return nil
}

// Fetch implements Saver.Fetch to fetch state of requested StateObject from Datastore.
func (ds *DatastoreSaver) Fetch(ctx context.Context, o StateObject) error {
	key := datastore.Key{Kind: o.Kind(), Name: o.Key(), Namespace: ds.Namespace}
	return ds.Client.Get(ctx, &key, o)
}

// FetchAll fetches all objects of a particular type from Datastore.
// The "o" parameter should be an instance of the type to fetch, which is
// used only to determine the kind, and to create the result slice.
func (ds *DatastoreSaver) FetchAll(ctx context.Context, o StateObject) ([]*datastore.Key, interface{}, error) {
	q := datastore.NewQuery(o.Kind()).Namespace(ds.Namespace)
	keys, err := ds.Client.GetAll(ctx, q.KeysOnly(), nil)
	if err != nil {
		return nil, nil, err
	}
	// Passing .Interface() to GetAll doesn't work, whether the slice is empty
	// or not, and we can't assert the correct type because we don't know it.
	// However, passing Interface() to GetMulti works just fine, so we use
	// that.

	// GetMulti accepts a slice as an interface{}, whereas GetAll does not.
	// It modifies the elements of the slice, and does not change the slice
	// itself.
	objs := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(o)), len(keys), len(keys)).Interface()
	err = ds.Client.GetMulti(ctx, keys, objs)
	if err != nil {
		return nil, nil, err
	}
	return keys, objs, err
}
