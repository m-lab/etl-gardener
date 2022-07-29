package persistence_test

import (
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
