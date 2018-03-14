package dispatch

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"time"

	"google.golang.org/api/option"

	"github.com/m-lab/etl-gardener/cloud/tq"
)

// ChannelQueueHandler is an autonomous queue handler running in a go
// routine, fed by a channel.
type ChannelQueueHandler struct {
	*tq.QueueHandler
	// Handler listens on this channel for prefixes.
	Channel chan string
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
func (chq *ChannelQueueHandler) waitForEmptyQueue() {
	// Don't want to accept a date until we can actually queue it.
	log.Println("Wait for empty queue ", chq.Queue)
	for err := chq.IsEmpty(); err != nil; err = chq.IsEmpty() {
		if err == tq.ErrMoreTasks {
			// Wait 5-15 seconds before checking again.
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		} else if err != nil {
			// We don't expect errors here, so try logging, and a large backoff
			// in case there is some bad network condition, service failure,
			// or perhaps the queue_pusher is down.
			log.Println(err)
			ErrorCount.WithLabelValues("IsEmptyError")
			// TODO update metric
			time.Sleep(time.Duration(60+rand.Intn(120)) * time.Second)
		}
	}
}

// processOneRequest waits on the channel for a new request, and handles it.
func (chq *ChannelQueueHandler) processOneRequest(prefix string, bucketOpts ...option.ClientOption) error {
	// Use proper storage bucket.
	parts, err := parsePrefix(prefix)
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		// TODO update metric
		FailCount.WithLabelValues("BadPrefix")
		return err
	}
	bucketName := parts[1]
	bucket, err := tq.GetBucket(bucketOpts, chq.Project, bucketName, false)
	if err != nil {
		log.Println(err)
		FailCount.WithLabelValues("BucketError")
		return err
	}
	chq.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
	return nil
}

// handleLoop processes requests on input channel
func (chq *ChannelQueueHandler) handleLoop(done chan<- bool, bucketOpts ...option.ClientOption) {
	log.Println("Starting handler for", chq.Queue)
	for {
		chq.waitForEmptyQueue()

		prefix, more := <-chq.Channel
		if more {
			err := chq.processOneRequest(prefix, bucketOpts...)
			if err == nil {
				log.Println("Dedup not implemented")
				FailCount.WithLabelValues("DedupNotImplemented")
			}
		} else {
			close(done)
			break
		}
	}
	log.Println("Exiting handler for", chq.Queue)
}

// StartHandleLoop starts a go routine that waits for work on channel, and
// processes it.
// Returns a channel that closes when input channel is closed and final processing is complete.
func (chq *ChannelQueueHandler) StartHandleLoop(bucketOpts ...option.ClientOption) <-chan bool {
	done := make(chan bool)
	go chq.handleLoop(done, bucketOpts...)
	return done
}

// NewChannelQueueHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewChannelQueueHandler(httpClient *http.Client, project, queue string, bucketOpts ...option.ClientOption) (chan<- string, <-chan bool, error) {
	qh, err := tq.NewQueueHandler(httpClient, project, queue)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan string)
	cqh := ChannelQueueHandler{qh, ch}

	done := cqh.StartHandleLoop(bucketOpts...)
	return ch, done, nil
}
