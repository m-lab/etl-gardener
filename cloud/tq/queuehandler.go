package tq

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"time"

	"github.com/m-lab/etl-gardener/api"
	"github.com/m-lab/etl-gardener/metrics"
	"google.golang.org/api/option"
)

// ChannelQueueHandler is an autonomous queue handler running in a go
// routine, fed by a channel.
type ChannelQueueHandler struct {
	*QueueHandler
	// Handler listens on this channel for prefixes.
	MsgChan      chan string
	ResponseChan chan error
}

// Sink returns the sink channel, for use by the sender.
func (qh *ChannelQueueHandler) Sink() chan<- string {
	return qh.MsgChan
}

// Response returns the response channel, that closes when all processing is complete.
func (qh *ChannelQueueHandler) Response() <-chan error {
	return qh.ResponseChan
}

func assertBasicPipe(ds api.BasicPipe) {
	func(ds api.BasicPipe) {}(&ChannelQueueHandler{})
}

const start = `^gs://(?P<bucket>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	prefixPattern = regexp.MustCompile(start + // #1 #2
		datePath) // #3 - YYYY/MM/DD
)

// ParsePrefix Parses prefix, returning {bucket, experiment, date string}, error
func ParsePrefix(prefix string) ([]string, error) {
	fields := prefixPattern.FindStringSubmatch(prefix)

	if fields == nil {
		return nil, errors.New("Invalid test path: " + prefix)
	}
	if len(fields) < 4 {
		return nil, errors.New("Path does not include all fields: " + prefix)
	}
	return fields, nil
}

// ErrChannelClosed is returned when the source channel closes.
var ErrChannelClosed = errors.New("source channel closed")

// waitForEmptyQueue loops checking queue until empty.
func (qh *ChannelQueueHandler) waitForEmptyQueue() {
	// Don't want to accept a date until we can actually queue it.
	log.Println("Wait for empty queue ", qh.Queue)
	for err := qh.IsEmpty(); err != nil; err = qh.IsEmpty() {
		if err == ErrMoreTasks {
			// Wait 5-15 seconds before checking again.
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		} else if err != nil {
			// We don't expect errors here, so try logging, and a large backoff
			// in case there is some bad network condition, service failure,
			// or perhaps the queue_pusher is down.
			log.Println(err)
			metrics.WarningCount.WithLabelValues("IsEmptyError").Inc()
			// TODO update metric
			time.Sleep(time.Duration(60+rand.Intn(120)) * time.Second)
		}
	}
}

// processOneRequest waits on the channel for a new request, and handles it.
// Returns number of items posted, or error
func (qh *ChannelQueueHandler) submitTasks(prefix string, bucketOpts ...option.ClientOption) (int, error) {
	// Use proper storage bucket.
	parts, err := ParsePrefix(prefix)

	// TODO - should we delete existing table first?  Issue #22

	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		// TODO update metric
		metrics.FailCount.WithLabelValues("BadPrefix").Inc()
		return 0, err
	}
	bucketName := parts[1]
	bucket, err := GetBucket(bucketOpts, qh.Project, bucketName, false)
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("BucketError").Inc()
		return 0, err
	}
	n, err := qh.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("PostDayError").Inc()
	}
	return n, err
}

// handleLoop processes requests on input channel
func (qh *ChannelQueueHandler) handleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	log.Println("Starting handler for", qh.Queue)
	// TODO - problem if we try to kill while waiting for empty queue.
	// That should probably be part of a select.
	qh.waitForEmptyQueue()
	for {
		prefix, more := <-qh.MsgChan
		if !more {
			close(qh.ResponseChan)
			break
		}

		// Submit the tasks
		n, err := qh.submitTasks(prefix, bucketOpts...)
		if err != nil {
			// TODO return error through Response()
		}

		// Must wait for empty queue before proceeding with dedupping.
		// This ensures that the data has actually been processed, rather
		// than just sitting in the queue or in the pipeline.
		qh.waitForEmptyQueue()
		// This may block if previous hasn't finished.  Should be rare.
		if n > 0 && next != nil {
			next.Sink() <- prefix
		}
	}
	log.Println("Exiting handler for", qh.Queue)
}

// StartHandleLoop starts a go routine that waits for work on channel, and
// processes it.
// Returns a channel that closes when input channel is closed and final processing is complete.
func (qh *ChannelQueueHandler) StartHandleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	go qh.handleLoop(next, bucketOpts...)
}

// NewChannelQueueHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewChannelQueueHandler(httpClient *http.Client, project, queue string, next api.BasicPipe, bucketOpts ...option.ClientOption) (*ChannelQueueHandler, error) {
	qh, err := NewQueueHandler(httpClient, project, queue)
	if err != nil {
		return nil, err
	}
	msg := make(chan string)
	rsp := make(chan error)
	cqh := ChannelQueueHandler{qh, msg, rsp}

	cqh.StartHandleLoop(next, bucketOpts...)
	return &cqh, nil
}
