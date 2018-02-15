// +build integration

package main

import (
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
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

func xTestStartup(t *testing.T) {
	client, err := datastore.NewClient(context.Background(), "mlab-testing", Options()...)
	if err != nil {
		t.Fatal(err)
	} else {
		dsClient = client
	}

	_, err = startupBatch("base-", 2)
	if err != nil {
		t.Fatal(err)
	}

	// TODO add tests for BatchState content once it has some.
}
