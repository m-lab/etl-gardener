// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	_ "expvar"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/osx"
)

// TODO - these tests currently fail with count=10
// Would have to use :0 for all servers to allow count > 1

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

	go func() {
		defer mainCancel()
		var resp *http.Response
		var err error
		for i := 0; i < 1000; i++ {
			time.Sleep(10 * time.Millisecond)
			resp, err = http.Get("http://localhost:8080/ready")
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		// For now, the service comes up immediately serving "ok" for /ready
		data, err := ioutil.ReadAll(resp.Body)
		if string(data) != "ok" {
			t.Fatal(string(data))
		}
	}()

	main()
}

func TestManagerMode(t *testing.T) {
	vars := map[string]string{
		"SERVICE_MODE":   "manager",
		"PROJECT":        "mlab-testing",
		"ARCHIVE_BUCKET": "archive-mlab-testing",
		"STATUS_PORT":    ":0",
	}
	for k, v := range vars {
		cleanup := osx.MustSetenv(k, v)
		defer cleanup()
	}

	go func() {
		defer mainCancel()
		var resp *http.Response
		var err error
		for i := 0; i < 1000; i++ {
			time.Sleep(10 * time.Millisecond)
			resp, err = http.Get("http://localhost:8080/ready")
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		// For now, the service comes up immediately serving "ok" for /ready
		data, err := ioutil.ReadAll(resp.Body)
		if string(data) != "ok" {
			t.Fatal(string(data))
		}
		resp.Body.Close()

		// Now get the status
		for i := 0; i < 1000; i++ {
			time.Sleep(10 * time.Millisecond)
			resp, err = http.Get("http://localhost:8080")
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatal(err)
		}

		data, err = ioutil.ReadAll(resp.Body)
		log.Print(string(data))
		resp.Body.Close()
	}()

	main()
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
