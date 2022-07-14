package persistence_test

import (
	"context"
	"log"
	"reflect"
	"testing"

	"github.com/m-lab/etl-gardener/persistence"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type O1 struct {
	persistence.Base

	Integer int32
}

// Kind implements StateObject.Kind
func (o O1) GetKind() string {
	return reflect.TypeOf(o).Name()
}

func NewO1(name string) O1 {
	return O1{Base: persistence.NewBase(name)}
}

func assertStateObject(so persistence.StateObject) {
	assertStateObject(O1{})
}

func TestNewLocalSaver(t *testing.T) {
	t.Run("create-save-fetch-delete", func(t *testing.T) {
		dir := t.TempDir()
		got := persistence.NewLocalSaver(dir)
		if got == nil {
			t.Fatalf("NewLocalSaver() = nil, want valid local saver.")
		}
		o := NewO1("foo-state")
		ctx := context.Background()
		o.Integer = 101 // Value to persist.

		err := got.Save(ctx, o)
		if err != nil {
			t.Errorf("LocalSaver.Save() = %v, want nil", err)
		}
		o2 := NewO1("foo-state")  // Without value.
		err = got.Fetch(ctx, &o2) // Read value.
		if err != nil {
			t.Errorf("LocalSaver.Fetch() = %v, want nil", err)
		}
		if o != o2 { // Compare values.
			t.Errorf("LocalSaver.Fetch() wrong result; got %v, want %v", o2, o)
		}
		err = got.Delete(ctx, o)
		if err != nil {
			t.Errorf("LocalSaver.Delete() = %v, want nil", err)
		}
		err = got.Fetch(ctx, &o2) // Read value after delete.
		if err == nil {
			t.Errorf("LocalSaver.Fetch() returned value after delete; got nil, want err")
		}
	})
}
