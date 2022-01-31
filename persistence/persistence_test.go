// +build integration

package persistence_test

import (
	"context"
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/go/rtx"
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

func TestDatastoreSaver(t *testing.T) {
	ctx := context.Background()
	ds, err := persistence.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}

	o := NewO1("foobar")
	o.Integer = 1234
	err = ds.Save(ctx, &o)
	if err != nil {
		t.Fatal(err)
	}

	o.Integer = 0
	err = ds.Fetch(ctx, &o)
	rtx.Must(err, "Fetch error")
	if o.Integer != 1234 {
		t.Error("Integer should be 1234", o)
	}

	err = ds.Delete(ctx, &o)
	rtx.Must(err, "Delete error")
	err = ds.Fetch(ctx, &o)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("Should have errored")
	}
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
