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
	MsgChan  chan string
	DoneChan <-chan bool
}

// Sink returns the sink channel, for use by the sender.
func (chq *ChannelQueueHandler) Sink() chan<- string {
	return chq.MsgChan
}

// Done returns the done channel, that closes when all processing is complete.
func (chq *ChannelQueueHandler) Done() <-chan bool {
	return chq.DoneChan
}

// Responses returns the response channel, that closes when all processing is complete.
func (chq *ChannelQueueHandler) Responses() <-chan error {
	return nil
}

func assertDownstream(ds api.Downstream) {
	assertDownstream(&ChannelQueueHandler{})
}

const start = `^gs://(?P<bucket>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	prefixPattern = regexp.MustCompile(start + // #1 #2
		datePath) // #3 - YYYY/MM/DD
)

// Parses prefix, returning {bucket, experiment, date string}, error
func parsePrefix(prefix string) ([]string, error) {
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
func (qh *ChannelQueueHandler) processOneRequest(prefix string, bucketOpts ...option.ClientOption) error {
	// Use proper storage bucket.
	parts, err := parsePrefix(prefix)
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		// TODO update metric
		metrics.FailCount.WithLabelValues("BadPrefix").Inc()
		return err
	}
	bucketName := parts[1]
	bucket, err := GetBucket(bucketOpts, qh.Project, bucketName, false)
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("BucketError").Inc()
		return err
	}
	qh.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
	return nil
}

// handleLoop processes requests on input channel
func (qh *ChannelQueueHandler) handleLoop(done chan<- bool, bucketOpts ...option.ClientOption) {
	log.Println("Starting handler for", qh.Queue)
	for {
		qh.waitForEmptyQueue()

		prefix, more := <-qh.Channel
		if !more {
			close(done)
			break
		}
		err := qh.processOneRequest(prefix, bucketOpts...)
		if err == nil {
			// TODO - replace with call to dedup handling code.
			log.Println("Dedup not implemented")
			metrics.FailCount.WithLabelValues("DedupNotImplemented").Inc()
		}
	}
	log.Println("Exiting handler for", qh.Queue)
}

// StartHandleLoop starts a go routine that waits for work on channel, and
// processes it.
// Returns a channel that closes when input channel is closed and final processing is complete.
func (qh *ChannelQueueHandler) StartHandleLoop(bucketOpts ...option.ClientOption) <-chan bool {
	done := make(chan bool)
	go qh.handleLoop(done, bucketOpts...)
	return done
}

// NewChannelQueueHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewChannelQueueHandler(httpClient *http.Client, project, queue string, bucketOpts ...option.ClientOption) (chan<- string, <-chan bool, error) {
	qh, err := NewQueueHandler(httpClient, project, queue)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan string)
	cqh := ChannelQueueHandler{qh, ch}

	done := cqh.StartHandleLoop(bucketOpts...)
	return ch, done, nil
}
