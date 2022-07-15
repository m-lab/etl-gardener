package persistence

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// StateObject defines the interface for objects to be saved/retrieved from persistent storage.
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

// Save implements Saver.Save using local files.
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

// Delete implements Saver.Delete using local files.
func (ls *LocalSaver) Delete(ctx context.Context, o StateObject) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	return os.Remove(ls.fname(o))
}

// Fetch implements Saver.Fetch to fetch state of requested StateObject from local files.
func (ls *LocalSaver) Fetch(ctx context.Context, o StateObject) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	b, err := ioutil.ReadFile(ls.fname(o))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, o)
}

// LocalNamedSaver is a generic saver interface for persisting to/from JSON
// objects.
type LocalNamedSaver struct {
	name string
	mx   sync.Mutex
}

// NewLocalNamedSaver creates a new LocalNamedSaver that saves results to the
// given file name.
func NewLocalNamedSaver(name string) *LocalNamedSaver {
	return &LocalNamedSaver{
		name: name,
	}
}

// Save serializes the given object as JSON and saves result.
func (ls *LocalNamedSaver) Save(v any) error {
	ls.mx.Lock()
	defer ls.mx.Unlock()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(ls.name, b, 0644)
}

// Load deserializes previously saved JSON content into the given object.
func (ls *LocalNamedSaver) Load(v any) error {
	ls.mx.Lock()
	defer ls.mx.Unlock()
	b, err := os.ReadFile(ls.name)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
