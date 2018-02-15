package main

import (
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/ds"
)

func Test_getDSClient(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	_, err := ds.NewSaver("gardener-test", "test")
	if err != nil {
		t.Fatal(err)
	}
}
