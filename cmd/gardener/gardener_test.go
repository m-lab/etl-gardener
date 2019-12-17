// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	"context"
	_ "expvar"
	"net/http"
	"testing"

	"github.com/m-lab/go/osx"
)

func TestLegacyModeSetup(t *testing.T) {
	vars := map[string]string{
		"PROJECT":         "mlab-testing",
		"QUEUE_BASE":      "fake-queue-",
		"NUM_QUEUES":      "123",
		"EXPERIMENT":      "ndt",
		"TASKFILE_BUCKET": "archive-mlab-testing",
		"START_DATE":      "20090220",
	}
	for k, v := range vars {
		cleanup := osx.MustSetenv(k, v)
		defer cleanup()
	}

	LoadEnv()
	_, err := taskHandlerFromEnv(context.Background(), http.DefaultClient)

	if err != nil {
		t.Fatal(err)
	}
}
