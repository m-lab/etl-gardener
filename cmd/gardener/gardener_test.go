// +build integration

// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	"context"
	_ "expvar"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/go/osx"
)

// Retries for up to 10 seconds.
func waitFor(url string) (resp *http.Response, err error) {
	for i := 0; i < 1000; i++ {
		time.Sleep(10 * time.Millisecond)
		resp, err = http.Get(url)
		if err == nil {
			break
		}
	}
	return resp, err
}

// TODO - these tests currently fail with count=10
// Would have to use :0 for all servers to allow count > 1

func TestLegacyModeSetup(t *testing.T) {
	mainCtx, mainCancel = context.WithCancel(context.Background())

	vars := map[string]string{
		"PROJECT":         "mlab-testing",
		"QUEUE_BASE":      "fake-queue-",
		"NUM_QUEUES":      "3",
		"EXPERIMENT":      "ndt",
		"TASKFILE_BUCKET": "archive-mlab-testing",
		"START_DATE":      "20090220",
	}
	for k, v := range vars {
		cleanup := osx.MustSetenv(k, v)
		defer cleanup()
	}

	go func() {
		defer mainCancel()
		resp, err := waitFor("http://localhost:8080/ready")
		if err != nil {
			t.Fatal(err)
		}
		// For now, the service comes up immediately serving "ok" for /ready
		data, err := ioutil.ReadAll(resp.Body)
		if string(data) != "ok" {
			t.Fatal(string(data))
		}
		resp.Body.Close()
	}()

	main()
}

func TestManagerMode(t *testing.T) {
	flag.Set("config_path", "testdata/config.yml")
	mainCtx, mainCancel = context.WithCancel(context.Background())

	vars := map[string]string{
		"SERVICE_MODE": "manager",
		"PROJECT":      "mlab-testing",
		"STATUS_PORT":  ":0",
	}
	for k, v := range vars {
		cleanup := osx.MustSetenv(k, v)
		defer cleanup()
	}

	go func(t *testing.T) {
		defer mainCancel()
		resp, err := waitFor("http://localhost:8080/ready")
		if err != nil {
			t.Fatal(err)
		}

		// For now, the service comes up immediately serving "ok" for /ready
		data, err := ioutil.ReadAll(resp.Body)
		if string(data) != "ok" {
			t.Fatal(string(data))
		}
		resp.Body.Close()
		log.Println("ok")

		// Now get the status
		resp, err = waitFor("http://" + statusServerAddr)
		if err != nil {
			t.Fatal(err)
		}
		data, err = ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(data), "Jobs") {
			t.Error("Should contain Jobs:\n", string(data))
		}
		resp.Body.Close()
	}(t)

	main()
}
