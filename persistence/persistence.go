package persistence

import (
	"encoding/json"
	"os"
	"sync"
)

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
