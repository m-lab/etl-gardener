// Package tq defines utilities for posting to task queues.
package tq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/cloud"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine/taskqueue"
)

// *******************************************************************
// QueueHandler handles queueing of reprocessing requests
// *******************************************************************

// Errors associated with Queuing
var (
	ErrNilClient        = errors.New("nil http client not allowed")
	ErrMoreTasks        = errors.New("queue has tasks pending")
	ErrTerminated       = errors.New("terminated early")
	ErrInvalidQueueName = errors.New("invalid queue name")
)

// QueueHandler is much like tq.Queuer, but for a single queue.  We want
// independent single queue handlers to avoid thread safety issues, among
// other things.
// It needs:
//   bucket
//   strategies for enqueuing.
type QueueHandler struct {
	cloud.Config
	Queue string // task queue name
}

// NewQueueHandler creates a QueueHandler struct from provided parameters.  This does network ops.
//   httpClient - client to be used for queue_pusher calls.  Allows injection of fake for testing.
//                must be non-null.
//   queue
func NewQueueHandler(config cloud.Config, queue string) (*QueueHandler, error) {
	if config.Client == nil {
		return nil, ErrNilClient
	}

	if strings.TrimSpace(queue) != queue {
		return nil, ErrInvalidQueueName
	}
	return &QueueHandler{config, queue}, nil
}

// IsEmptyBuggy checks whether the queue is empty, i.e., all tasks have been successfully
// processed.
// NOTE: This is unreliable.  Taskqueue Stats may return an empty stats object.
func (qh *QueueHandler) IsEmptyBuggy() error {
	stats, err := GetTaskqueueStats(qh.Config, qh.Queue)
	if err != nil {
		return err
	}
	if stats.InFlight+stats.Tasks != 0 {
		return ErrMoreTasks
	}
	return nil
}

// GetTaskqueueStats gets stats for a single task queue.
// NOTE: This is unreliable, possibly returning an empty stats object, when the actual QueueStatistics
// are not empty.  Caller should not trust empty return value.
func GetTaskqueueStats(config cloud.Config, name string) (stats taskqueue.QueueStatistics, err error) {
	// Would prefer to use this, but it does not work from flex![]
	// stats, err := taskqueue.QueueStats(config.Context, queueNames)
	resp, err := config.Client.Get(fmt.Sprintf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, config.Project, name))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = errors.New("HTTP request: " + http.StatusText(resp.StatusCode))
		log.Println(err)
		log.Printf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s\n`, config.Project, name)
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
		log.Printf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, config.Project, name)
		return
	}
	if len(statsSlice) != 1 {
		err = errors.New("wrong length statsSlice")
	}
	stats = statsSlice[0]
	return
}

// WaitForEmptyQueue loops checking queue until empty.
// There is a bug in task queue status (from AppEngine) which causes it to
// occasionally (a few times a day) return all zeros. This erronious report
// seems to persist for a minute or more.  This is indistinguishable
// from an actual empty queue, so we use a slightly different criteria:
//  1. If queue was most recently 0 pending, >0 in flight, then we trust
//     the empty queue state.
//  2. If queue most recently had >0 pending, then we assume a zero state may
//     be spurious, and check two more times over the next two minutes or so.
//  3. If the queue appears empty (or errors) for 3 minutes, then we return,
//     basically assuming it is empty now.
func (qh *QueueHandler) WaitForEmptyQueue(terminate <-chan struct{}) error {
	log.Println("Wait for empty queue ", qh.Queue)
	empty := taskqueue.QueueStatistics{}
	lastNonEmpty := taskqueue.QueueStatistics{}
	lastNonEmptyTime := time.Now()

	// If the last non-empty stats are close to being empty, then we are more trusting
	// if we see an empty stats.  But if lsatNonEmpty is actually empty, then not so much.
	for {
		select {
		case <-terminate:
			return ErrTerminated
		default:
			stats, err := GetTaskqueueStats(qh.Config, qh.Queue)
			if err != nil {
				if err == io.EOF {
					log.Println(err, "GetTaskqueueStats returned EOF - test client?")
				}
				return err
			}
			if stats == empty {
				// Empty stats are not trustworthy, so we do more sanity checking.
				if lastNonEmpty != empty && lastNonEmpty.Tasks == 0 {
					// Most likely we really are done now.  Even if something is still
					// in flight, we will just assume it is likely to finish.
					log.Printf("%s Previous %+v  Current %+v", qh.Queue, lastNonEmpty, stats)
					return nil
				}

				if time.Since(lastNonEmptyTime) > 3*time.Minute {
					// Its been this way for at least 3 minutes, and at least 2 samples.
					// Probably OK.
					log.Printf("%s TIMEOUT:  Previous %+v  Current %+v", qh.Queue, lastNonEmpty, stats)
					return nil
				}

				log.Printf("Suspicious (%s): %+v\n", qh.Queue, stats)
			} else {
				if stats.InFlight+stats.Tasks == 0 {
					// This is a trusted zero result!
					log.Printf("%s Previous %+v  Current %+v", qh.Queue, lastNonEmpty, stats)
					return nil
				}
				lastNonEmpty = stats
				lastNonEmptyTime = time.Now()
			}

			// Check about once every minute.
			time.Sleep(time.Duration(30+rand.Intn(60)) * time.Second)
		}
	}
}

// PostOneTask sends a single https request to the queue pusher to add a task.
// TODO - should use AddMulti - should be much faster.
//   however - be careful not to exceed quotas
func (qh QueueHandler) PostOneTask(bucket, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", qh.Project, qh.Queue, bucket, fn)

	resp, err := qh.Client.Get(reqStr)
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
	backoff := 50 * time.Second
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
// returns (fileCount, byteCount, error)
func (qh *QueueHandler) PostAll(bucket string, it stiface.ObjectIterator) (int, int64, error) {
	fileCount := 0
	byteCount := int64(0)
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
				return fileCount, byteCount, err
			}
			continue
		}

		err = qh.postWithRetry(bucket, o.Name)
		if err != nil {
			log.Println(err, "attempting to post", o.Name, "to", qh.Queue)
			qpErrCount++
			if qpErrCount > 5 {
				log.Printf("Failed after %d files to %s (on %s).\n", fileCount, qh.Queue, o.Name)
				return fileCount, byteCount, err
			}
		} else {
			fileCount++
			byteCount += o.Size
		}
	}
	return fileCount, byteCount, nil
}

// PostDay fetches an iterator over the objects with ndt/YYYY/MM/DD prefix,
// and passes the iterator to postDay with appropriate queue.
// This typically takes about 10 minutes for a 20K task NDT day.
func (qh *QueueHandler) PostDay(ctx context.Context, bucket stiface.BucketHandle, bucketName, prefix string) (int, int64, error) {
	log.Println("Adding ", prefix, " to ", qh.Queue)
	qry := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - handle timeout errors?
	// TODO - should we add a deadline?
	it := bucket.Objects(ctx, &qry)
	fileCount, byteCount, err := qh.PostAll(bucketName, it)

	log.Println("Added ", fileCount, "tasks from", prefix, " to ", qh.Queue)
	return fileCount, byteCount, err
}

// *******************************************************************
// Storage Bucket related stuff.
//  TODO move to another package?
// *******************************************************************

// GetBucket gets a storage bucket.
//   opts       - ClientOptions, e.g. credentials, for tests that need to access storage buckets.
func GetBucket(ctx context.Context, sClient stiface.Client, project, bucketName string, dryRun bool) (stiface.BucketHandle, error) {
	bucket := sClient.Bucket(bucketName)
	// Check that the bucket is valid, by fetching it's attributes.
	// Bypass check if we are running travis tests.
	if !dryRun {
		_, err := bucket.Attrs(ctx)
		if err != nil {
			return nil, err
		}
	}
	return bucket, nil
}
