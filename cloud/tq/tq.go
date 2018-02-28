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
	"sync"
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
	count int
}

// Count returns the client call count.
func (ct *CountingTransport) Count() int {
	return ct.count
}

type nopCloser struct {
	io.Reader
}

func (nc *nopCloser) Close() error { return nil }

// RoundTrip implements the RoundTripper interface, logging the
// request, and the response body, (which may be json).
func (ct *CountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ct.count++
	resp := &http.Response{}
	resp.StatusCode = http.StatusOK
	resp.Body = &nopCloser{strings.NewReader("")}
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

// A Queuer provides the environment for queuing reprocessing requests.
type Queuer struct {
	Project   string // project containing task queue
	QueueBase string // task queue base name
	NumQueues int    // number of task queues.
	// TODO - move bucket related stuff elsewhere.
	xBucketName string                // name of bucket containing task files
	xBucket     *storage.BucketHandle // bucket handle
	HTTPClient  *http.Client          // Client to be used for http requests to queue pusher.
}

// ErrNilClient is returned when client is not provided.
var ErrNilClient = errors.New("nil http client not allowed")

// CreateQueuer creates a Queuer struct from provided parameters.  This does network ops.
//   httpClient - client to be used for queue_pusher calls.  Allows injection of fake for testing.
//                must be non-null.
//   opts       - ClientOptions, e.g. credentials, for tests that need to access storage buckets.
func CreateQueuer(httpClient *http.Client, opts []option.ClientOption, queueBase string, numQueues int, project, bucketName string, dryRun bool) (Queuer, error) {
	if httpClient == nil {
		return Queuer{}, ErrNilClient
	}

	bucket, err := GetBucket(opts, project, bucketName, dryRun)
	if err != nil {
		return Queuer{}, err
	}

	return Queuer{project, queueBase, numQueues, bucketName, bucket, httpClient}, nil
}

// GetTaskqueueStats gets stats for a single task queue.
func GetTaskqueueStats(project string, name string) (stats taskqueue.QueueStatistics, err error) {
	// Would prefer to use this, but it does not work from flex![]
	// stats, err := taskqueue.QueueStats(context.Background(), queueNames)
	resp, err := http.Get(fmt.Sprintf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, project, name))
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

// GetTaskqueueStats returns the stats for a list of queues.
// It should return a taskqueue.QueueStatistics for each queue.
// If any errors occur, the last error will be returned.
//  On request error, it will return a partial map (usually empty) and the error.
// If there are missing queues or parse errors, it will add an empty QueueStatistics
// object to the map, and continue, saving the error for later return.
// TODO update queue_pusher to process multiple queue names in one request.
// TODO add unit test.
func (q *Queuer) GetTaskqueueStats() (map[string]taskqueue.QueueStatistics, error) {
	queueNames := make([]string, q.NumQueues)
	for i := range queueNames {
		queueNames[i] = fmt.Sprintf("%s%d", q.QueueBase, i)
	}
	allStats := make(map[string]taskqueue.QueueStatistics, len(queueNames))
	var finalErr error
	for i := range queueNames {
		stats, err := GetTaskqueueStats(q.Project, queueNames[i])
		if err != nil {
			finalErr = err
		} else {
			allStats[queueNames[i]] = stats
		}
	}
	return allStats, finalErr
}

// Initially this used a hash, but using day ordinal is better
// as it distributes across the queues more evenly.
func (q Queuer) queueForDate(date time.Time) string {
	day := date.Unix() / (24 * 60 * 60)
	return fmt.Sprintf("%s%d", q.QueueBase, int(day)%q.NumQueues)
}

// PostOneTask sends a single https request to the queue pusher to add a task.
// Iff dryRun is true, this does nothing.
// TODO - move retry into this method.
// TODO - should use AddMulti - should be much faster.
//   however - be careful not to exceed quotas
func (q Queuer) PostOneTask(queue, bucket, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", q.Project, queue, bucket, fn)

	resp, err := q.HTTPClient.Get(reqStr)
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
			return errors.New(message + " " + queue)
		}
		log.Println(string(buf))
		return errors.New(resp.Status + " :: " + message)
	}

	return nil
}

// postWithRetry posts a single task to a task queue.  It will make up to 3 attempts
// if there are recoverable errors.
func (q *Queuer) postWithRetry(queue string, bucket, filepath string) error {
	backoff := 5 * time.Second
	var err error
	for i := 0; i < 3; i++ {
		err = q.PostOneTask(queue, bucket, filepath)
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
func (q *Queuer) PostAll(wg *sync.WaitGroup, queue string, bucket string, it *storage.ObjectIterator) error {
	if wg != nil {
		defer wg.Done()
	}

	fileCount := 0
	defer func() {
		log.Println("Added ", fileCount, " tasks to ", queue)
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
				log.Printf("Failed after %d files to %s.\n", fileCount, queue)
				return err
			}
			continue
		}

		err = q.postWithRetry(queue, bucket, o.Name)
		if err != nil {
			log.Println(err, "attempting to post", o.Name, "to", queue)
			qpErrCount++
			if qpErrCount > 5 {
				log.Printf("Failed after %d files to %s (on %s).\n", fileCount, queue, o.Name)
				return err
			}
		} else {
			fileCount++
		}
	}
	return nil
}

// PostDay fetches an iterator over the objects with ndt/YYYY/MM/DD prefix,
// and passes the iterator to postDay with appropriate queue.
// Iff wq is not nil, PostDay will call done on wg when finished
// posting.
// This typically takes about 10 minutes, whether single or parallel days.
func (q Queuer) PostDay(wg *sync.WaitGroup, bucket *storage.BucketHandle, bucketName, prefix string) error {
	date, err := time.Parse("ndt/2006/01/02/", prefix)
	if err != nil {
		log.Println("Failed parsing date from ", prefix)
		log.Println(err)
		if wg != nil {
			wg.Done()
		}
		return err
	}
	queue := q.queueForDate(date)
	log.Println("Adding ", prefix, " to ", queue)
	qry := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - can this error?  Or do errors only occur on iterator ops?
	it := bucket.Objects(context.Background(), &qry)
	if wg != nil {
		// TODO - this ignores errors.
		go q.PostAll(wg, queue, bucketName, it)
	} else {
		return q.PostAll(nil, queue, bucketName, it)
	}
	return nil
}

// ###############################################################################
//  Batch processing task scheduling and support code
// ###############################################################################

// ReprocState represents the current processing state for a partition reprocessing job.
type ReprocState int

const (
	// Pending - job created and stored but not started.
	Pending ReprocState = iota
	// Queuing - tasks are being posted to Queue
	Queuing
	// Processing - all tasks are queued but not completed
	Processing
	// Dedupping - all tasks completed, submitting dedup query
	Dedupping
	// CopyPending - dedup query complete, validating/copying
	CopyPending
	// Done - copy and delete completed, job can be removed.
	Done
)

// PartitionState holds the complete state information for processing a single day partition.
type PartitionState struct {
	DSKey      string      // Key used for DataStore
	State      ReprocState // Current state of the partition processing.
	Date       string      // YYYYMMDD partition suffix for the date.
	QueueIndex int         // index of the queue for this task.
	NextTask   string      // the "gs://" task_filename of the next task to be submitted to the queue.
	Restart    bool        // Indicates when new owner is recovering.
}

// QueueHandler contains elements and methods for all processing for a single queue.
type QueueHandler struct {
	BucketHandle    *storage.BucketHandle
	BucketName      string
	QueueName       string              // Name of the target task queue
	Queuer          *Queuer             // Queuer used for all queuing
	QueueAndProcess chan PartitionState // Channel from dispatcher
	DedupAndCleanup chan PartitionState // Channel between queuer and dedupper
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
