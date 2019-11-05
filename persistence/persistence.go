package persistence

import (
	"context"
	"time"

	"cloud.google.com/go/datastore"
)

// StateObject defines the interface for objects to be saved/retrieved from datastore.
type StateObject interface {
	Name() string
	Kind() string // Should be implemented in the actual type, as return reflect.TypeOf(o).Name()
}

// Base is the base for persistent objects.  All StateObjects should embed
// Base and call NewBase to initialize.
type Base struct {
	name string
}

// Name implements StateObject.Name
func (o Base) Name() string {
	return o.name
}

// NewBase initializes a Base object.
func NewBase(name string) Base {
	return Base{name: name}
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
	k := datastore.NameKey(o.Kind(), o.Name(), nil)
	k.Namespace = ds.Namespace
	return k
}

// Save implements Saver.Save using Datastore.
func (ds *DatastoreSaver) Save(ctx context.Context, o StateObject) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := ds.Client.Put(ctx, ds.key(o), o)
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Delete implements Saver.Delete using Datastore.
func (ds *DatastoreSaver) Delete(ctx context.Context, o StateObject) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := ds.Client.Delete(ctx, ds.key(o))
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Fetch implements Saver.Fetch to fetch state of requested StateObject from Datastore.
func (ds *DatastoreSaver) Fetch(ctx context.Context, o StateObject) error {
	key := datastore.Key{Kind: o.Kind(), Name: o.Name(), Namespace: ds.Namespace}
	return ds.Client.Get(ctx, &key, o)
}
