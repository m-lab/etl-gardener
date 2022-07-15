package persistence_test

import (
	"context"
	"log"
	"os"
	"path"
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

func TestLocalNamedSaver(t *testing.T) {
	type foo struct {
		A int
		B float64
		C string
	}
	tests := []struct {
		name        string
		input       any
		output      any
		removeDir   bool
		wantSaveErr bool
		removeFile  bool
		wantLoadErr bool
	}{
		{
			name:   "successful-save-and-load",
			input:  &foo{A: 10},
			output: &foo{},
		},
		{
			name:        "save-error-unsupported-type",
			input:       struct{ F func() }{}, // invalid field.
			wantSaveErr: true,
		},
		{
			name:        "save-error-no-write-permission",
			input:       &foo{A: 10},
			removeDir:   true,
			wantSaveErr: true,
		},
		{
			name:        "load-error-missing-file",
			input:       &foo{A: 10},
			output:      &foo{},
			removeFile:  true,
			wantLoadErr: true,
		},
		{
			name:        "load-error-wrong-target-type",
			input:       &foo{A: 10},
			output:      struct{ F func() }{},
			wantLoadErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			file := path.Join(dir, "output.json")
			if tt.removeDir {
				os.Remove(dir) // make directory unwritable.
			}
			// save to named saver.
			ls := persistence.NewLocalNamedSaver(file)
			err := ls.Save(tt.input)
			if (err != nil) != tt.wantSaveErr {
				t.Errorf("Save() got unexpected error; got %v, wantSaveErr %t", err, tt.wantSaveErr)
			}
			if tt.wantSaveErr {
				return
			}

			if tt.removeFile {
				os.Remove(file) // remove saved file so load fails.
			}
			err = ls.Load(tt.output)
			if !tt.wantLoadErr && (err != nil) {
				t.Errorf("Load() got unexpected error; got %v, want nil", err)
			}
			if tt.wantLoadErr {
				return
			}

			// Compare intput and output structures to verify data is loaded correctly.
			if !reflect.DeepEqual(tt.input, tt.output) {
				t.Errorf("Load() got different values; got %v, want %v", tt.output, tt.input)
			}
		})
	}
}
