// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	"context"
	_ "expvar"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

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

func TestManagerMode(t *testing.T) {
	vars := map[string]string{
		"SERVICE_MODE":   "manager",
		"PROJECT":        "foobar",
		"ARCHIVE_BUCKET": "archive-mlab-testing",
		"STATUS_PORT":    ":0",
	}
	for k, v := range vars {
		cleanup := osx.MustSetenv(k, v)
		defer cleanup()
	}

	go main()

	time.Sleep(time.Second)
	resp, err := http.Get("http://localhost:8080/ready")
	if err != nil {
		t.Fatal(err)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if string(data) != "ok" {
		t.Fatal(string(data))
	}
}

func TestEnv(t *testing.T) {
	vars := map[string]string{
		"SERVICE_MODE":   "manager",
		"PROJECT":        "foobar",
		"ARCHIVE_BUCKET": "archive-mlab-testing",
	}
	for k := range vars {
		v := os.Getenv(k)
		log.Println(k, v)
	}
}
