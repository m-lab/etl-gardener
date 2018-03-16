// Package tq defines utilities for posting to task queues.
package tq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/appengine/taskqueue"
)

// *******************************************************************
// DryRunQueuerClient, that just returns status ok and empty body
// For injection when we want Queuer to do nothing.
// *******************************************************************

// CountingTransport counts calls, and returns OK and empty body.
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

// DryRunQueuerClient returns a client that just counts calls.
func DryRunQueuerClient() (*http.Client, *CountingTransport) {
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

// *******************************************************************
// Queuer handles queueing of reprocessing requests
// *******************************************************************

// Errors associated with Queuing
var (
	ErrNilClient = errors.New("nil http client not allowed")
	ErrMoreTasks = errors.New("queue has tasks pending")
)

// QueueHandler is much like tq.Queuer, but for a single queue.  We want
// independent single queue handlers to avoid thread safety issues, among
// other things.
// It needs:
//   bucket
//   strategies for enqueuing.
type QueueHandler struct {
	Project string // project containing task queue
	Queue   string // task queue name
	// Bucket    *storage.BucketHandle
	HTTPClient *http.Client // Client to be used for http requests to queue pusher.
}

// NewQueueHandler creates a QueueHandler struct from provided parameters.  This does network ops.
//   httpClient - client to be used for queue_pusher calls.  Allows injection of fake for testing.
//                must be non-null.
//   project
//   queue
func NewQueueHandler(httpClient *http.Client, project, queue string) (*QueueHandler, error) {
	if httpClient == nil {
		return nil, ErrNilClient
	}

	return &QueueHandler{project, queue, httpClient}, nil
}

// IsEmpty checks whether the queue is empty, i.e., all tasks have been successfully
// processed.
func (qh *QueueHandler) IsEmpty() error {
	stats, err := GetTaskqueueStats(qh.HTTPClient, qh.Project, qh.Queue)
	if err != nil {
		return err
	}
	if stats.InFlight+stats.Tasks != 0 {
		return ErrMoreTasks
	}
	return nil
}

// GetTaskqueueStats gets stats for a single task queue.
func GetTaskqueueStats(client *http.Client, project string, name string) (stats taskqueue.QueueStatistics, err error) {
	// Would prefer to use this, but it does not work from flex![]
	// stats, err := taskqueue.QueueStats(context.Background(), queueNames)
	resp, err := client.Get(fmt.Sprintf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, project, name))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("HTTP request: " + http.StatusText(resp.StatusCode))
		log.Println(err)
		log.Printf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s\n`, project, name)
		return
	}
	data := make([]byte, 10000)
	var n int
	n, err = io.ReadFull(resp.Body, data)
	if err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			log.Println(err, "No content from task queue request - test client?")
			err = nil
		}
		return
	}
	var statsSlice []taskqueue.QueueStatistics
	err = json.Unmarshal(data[:n], &statsSlice)
	if err != nil {
		log.Println(err)
		log.Printf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, project, name)
		return
	}
	if len(statsSlice) != 1 {
		err = errors.New("wrong length statsSlice")
	}
	stats = statsSlice[0]
	return
}

// PostOneTask sends a single https request to the queue pusher to add a task.
// TODO - should use AddMulti - should be much faster.
//   however - be careful not to exceed quotas
func (qh QueueHandler) PostOneTask(bucket, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", qh.Project, qh.Queue, bucket, fn)

	resp, err := qh.HTTPClient.Get(reqStr)
	if err != nil {
		log.Println(err)
		// TODO - we don't see errors here or below when the queue doesn't exist.
		// That seems bad.
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		message := string(buf)
		if strings.Contains(message, "UNKNOWN_QUEUE") {
			return errors.New(message + " " + qh.Queue)
		}
		log.Println(string(buf))
		return errors.New(resp.Status + " :: " + message)
	}

	return nil
}

// postWithRetry posts a single task to a task queue.  It will make up to 3 attempts
// if there are recoverable errors.
func (qh *QueueHandler) postWithRetry(bucket, filepath string) error {
	backoff := 5 * time.Second
	var err error
	for i := 0; i < 3; i++ {
		err = qh.PostOneTask(bucket, filepath)
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "UNKNOWN_QUEUE") ||
			strings.Contains(err.Error(), "invalid filename") {
			// These error types will never recover.
			return err
		}
		log.Println(err, filepath, "Retrying")
		// Backoff and retry.
		time.Sleep(backoff)
		backoff = 2 * backoff
	}
	return err
}

// PostAll posts all normal file items in an ObjectIterator into the appropriate queue.
func (qh *QueueHandler) PostAll(bucket string, it *storage.ObjectIterator) (int, error) {
	fileCount := 0
	defer func() {
		log.Println("Added ", fileCount, " tasks to ", qh.Queue)
	}()

	qpErrCount := 0
	gcsErrCount := 0
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			// TODO - should this retry?
			// log the underlying error, with added context
			log.Println(err, "when attempting it.Next()")
			gcsErrCount++
			if gcsErrCount > 5 {
				log.Printf("Failed after %d files to %s.\n", fileCount, qh.Queue)
				return 0, err
			}
			continue
		}

		err = qh.postWithRetry(bucket, o.Name)
		if err != nil {
			log.Println(err, "attempting to post", o.Name, "to", qh.Queue)
			qpErrCount++
			if qpErrCount > 5 {
				log.Printf("Failed after %d files to %s (on %s).\n", fileCount, qh.Queue, o.Name)
				return 0, err
			}
		} else {
			fileCount++
		}
	}
	return fileCount, nil
}

// PostDay fetches an iterator over the objects with ndt/YYYY/MM/DD prefix,
// and passes the iterator to postDay with appropriate queue.
// This typically takes about 10 minutes for a 20K task NDT day.
// TODO return the count of items posted.
func (qh *QueueHandler) PostDay(bucket *storage.BucketHandle, bucketName, prefix string) (int, error) {
	log.Println("Adding ", prefix, " to ", qh.Queue)
	qry := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - can this error?  Or do errors only occur on iterator ops?
	it := bucket.Objects(context.Background(), &qry)
	return qh.PostAll(bucketName, it)
}

// *******************************************************************
// Storage Bucket related stuff.
//  TODO move to another package?
// *******************************************************************

// GetBucket gets a storage bucket.
//   opts       - ClientOptions, e.g. credentials, for tests that need to access storage buckets.
func GetBucket(opts []option.ClientOption, project, bucketName string, dryRun bool) (*storage.BucketHandle, error) {
	storageClient, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	bucket := storageClient.Bucket(bucketName)
	// Check that the bucket is valid, by fetching it's attributes.
	// Bypass check if we are running travis tests.
	if !dryRun {
		_, err = bucket.Attrs(context.Background())
		if err != nil {
			return nil, err
		}
	}
	return bucket, nil
}
