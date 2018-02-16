package main

import (
	"os"
	"testing"

	"google.golang.org/api/option"
)

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

func Test_getDSClient(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	c, err := getDSClient(Options()...)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Error("Should be non-nil client")
	}
}
