package main

import (
	"os"
	"testing"
)

// Currently failing because of auth.
func xTest_getDSClient(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	c, err := getDSClient()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Error("Should be non-nil client")
	}
}
