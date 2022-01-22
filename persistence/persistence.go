package persistence

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
)

// StateObject defines the interface for objects to be saved/retrieved from datastore.
type StateObject interface {
	GetName() string
	// Should be implemented in the actual type, as
	// func (o ConcreteType) GetKind() string {
	//   return reflect.TypeOf(o).String()
	// }
	GetKind() string
}

// Base is the base for persistent objects.  All StateObjects should embed
// Base and call NewBase to initialize.
type Base struct {
	Name string
}

// GetName implements StateObject.Name
func (o Base) GetName() string {
	return o.Name
}

// NewBase initializes a Base object.
func NewBase(name string) Base {
	return Base{Name: name}
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
	return &DatastoreSaver{Client: client, Namespace: "gardener"}, nil
}

func (ds *DatastoreSaver) key(o StateObject) *datastore.Key {
	k := datastore.NameKey(o.GetKind(), o.GetName(), nil)
	k.Namespace = ds.Namespace
	return k
}

// Save implements Saver.Save using Datastore.
func (ds *DatastoreSaver) Save(ctx context.Context, o StateObject) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := ds.Client.Put(ctx, ds.key(o), o)
	if err != nil {
		log.Println("Save error", err, ds.key(o))
		return err
	}
	return nil
}

// Delete implements Saver.Delete using Datastore.
func (ds *DatastoreSaver) Delete(ctx context.Context, o StateObject) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := ds.Client.Delete(ctx, ds.key(o))
	if err != nil {
		log.Println("Delete error", err, ds.key(o))
		return err
	}
	return nil
}

// Fetch implements Saver.Fetch to fetch state of requested StateObject from Datastore.
func (ds *DatastoreSaver) Fetch(ctx context.Context, o StateObject) error {
	key := datastore.Key{Kind: o.GetKind(), Name: o.GetName(), Namespace: ds.Namespace}
	return ds.Client.Get(ctx, &key, o)
}

// LocalSaver implements a Saver that stores state objects in local files.
type LocalSaver struct {
	Namespace string
	dir       string
	lock      sync.Mutex
}

// NewLocalSaver creates and returns a new local saver.
func NewLocalSaver(dir string) *LocalSaver {
	return &LocalSaver{dir: dir, Namespace: "gardener"}
}

func (ls *LocalSaver) fname(o StateObject) string {
	return path.Join(ls.dir, ls.Namespace+"-"+o.GetKind()+"-"+o.GetName())
}

// Save implements Saver.Save using Datastore.
func (ls *LocalSaver) Save(ctx context.Context, o StateObject) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	f, err := os.OpenFile(ls.fname(o), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	b, err := json.Marshal(o)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}

// Delete implements Saver.Delete using Datastore.
func (ls *LocalSaver) Delete(ctx context.Context, o StateObject) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	return os.Remove(ls.fname(o))
}

// Fetch implements Saver.Fetch to fetch state of requested StateObject from Datastore.
func (ls *LocalSaver) Fetch(ctx context.Context, o StateObject) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	b, err := ioutil.ReadFile(ls.fname(o))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, o)
}
