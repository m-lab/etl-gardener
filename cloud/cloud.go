// Package cloud contains a generic cloud Config, and utilities like
// DryRunClient.
package cloud

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"

	"google.golang.org/api/option"
)

// Config provides a generic config suitable for many cloud clients.
type Config struct {
	Project string // Project for most cloud ops.

	// client to be used for cloud API calls.  Allows injection of fake for testing.
	Client  *http.Client
	Options []option.ClientOption
}

// BQConfig provides a generic config suitable for BigQuery clients.
type BQConfig struct {
	Config

	// In prod, most services are from mlab-oti, but BQ tables may be in measurement-lab.
	// So we provide a separate BQProject
	BQProject string // Project for BigQuery tables
	BQDataset string // Dataset for BigQuery tables
}

// *******************************************************************
// DryRunQueuerClient, that just returns status ok and empty body
// For injection when we want Queuer to do nothing.
// *******************************************************************

// CountingTransport counts calls, and returns OK and empty body.
// `count` field should only be accessed using atomic.Foobar
type CountingTransport struct {
	count int32
	reqs  []*http.Request
}

// Count returns the client call count.
func (ct *CountingTransport) Count() int32 {
	return atomic.LoadInt32(&ct.count)
}

// Requests returns the entire req from the last request
func (ct *CountingTransport) Requests() []*http.Request {
	return ct.reqs
}

type nopCloser struct {
	io.Reader
}

func (nc *nopCloser) Close() error { return nil }

// RoundTrip implements the RoundTripper interface, logging the
// request, and the response body, (which may be json).
func (ct *CountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	atomic.AddInt32(&ct.count, 1)

	// Create an empty response with StatusOK
	resp := &http.Response{}
	resp.StatusCode = http.StatusOK
	resp.Body = &nopCloser{strings.NewReader("")}

	// Save the request for testing.
	ct.reqs = append(ct.reqs, req)

	return resp, nil
}

// DryRunClient returns a client that just counts calls.
func DryRunClient() (*http.Client, *CountingTransport) {
	client := &http.Client{}
	tp := &CountingTransport{}
	client.Transport = tp
	return client, tp
}

// This is used to intercept Get requests to the queue_pusher when invoked
// with -dry_run.
type dryRunHTTP struct{}

func (dr *dryRunHTTP) Get(url string) (resp *http.Response, err error) {
	resp = &http.Response{}
	resp.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))
	resp.Status = "200 OK"
	resp.StatusCode = 200
	return
}
