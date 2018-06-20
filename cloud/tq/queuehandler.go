package tq

import (
	"errors"
	"io"
	"log"
	"math/rand"
	"regexp"
	"time"

	"github.com/m-lab/etl-gardener/api"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"google.golang.org/api/option"
	"google.golang.org/appengine/taskqueue"
)

// ChannelQueueHandler is an autonomous queue handler running in a go
// routine, fed by a channel.
type ChannelQueueHandler struct {
	cloud.Config // Package level config options

	*QueueHandler // NOTE: This also contains a Project field.
	// Handler listens on this channel for prefixes.
	MsgChan      chan state.Task
	ResponseChan chan error
}

// Sink returns the sink channel, for use by the sender.
func (qh *ChannelQueueHandler) Sink() chan<- state.Task {
	return qh.MsgChan
}

// Response returns the response channel, that closes when all processing is complete.
func (qh *ChannelQueueHandler) Response() <-chan error {
	return qh.ResponseChan
}

func assertTaskPipe(ds api.TaskPipe) {
	func(ds api.TaskPipe) {}(&ChannelQueueHandler{})
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
func (qh *ChannelQueueHandler) waitForEmptyQueue() {
	// Don't want to accept a date until we can actually queue it.
	log.Println("Wait for empty queue ", qh.Queue)
	var lastTrusted taskqueue.QueueStatistics
	inactiveStartTime := time.Now() // If the queue is actually empty, this allows timeout.
	nullTime := time.Time{}
	for {
		stats, err := GetTaskqueueStats(qh.HTTPClient, qh.Config.Project, qh.Queue)
		if err != nil {
			if err == io.EOF {
				if !qh.TestMode {
					log.Println(err, "GetTaskqueueStats returned EOF - test client?")
				}
				return
			}
			// We don't expect errors here, so log and retry,
			// in case there is some bad network condition, service failure,
			// or perhaps the queue_pusher is down.
			log.Println(err)
			metrics.WarningCount.WithLabelValues("ErrGetTaskqueueStats").Inc()
		} else if stats.Tasks > 0 || stats.InFlight > 0 {
			// Good data, queue is not empty...
			lastTrusted = stats
			inactiveStartTime = nullTime
		} else if stats.Executed1Minute > 0 {
			log.Printf("Looks good (%s): %+v vs %+v", qh.Queue, stats, lastTrusted)
			break // Likely valid empty queue.
		} else {
			// Record the first time we see an apparently empty queue.
			if inactiveStartTime == nullTime {
				inactiveStartTime = time.Now()
			}
			if lastTrusted.Tasks == 0 {
				// Most likely we really are done now.  Even if something is still
				// in flight, we will just assume it is likely to finish.
				break
			}
			log.Printf("Suspicious (%s): %+v\n", qh.Queue, stats)
		}
		if inactiveStartTime != nullTime && time.Since(inactiveStartTime) > 180*time.Second {
			// It's been long enough to assume the queue is really empty.
			// Or possibly we've just been getting errors all this time.
			log.Printf("Timeout. (%s) Last trusted was: %+v", qh.Queue, lastTrusted)
			break
		}
		// Check about once every minute.
		time.Sleep(time.Duration(30+rand.Intn(60)) * time.Second)
	}
}

// processOneRequest waits on the channel for a new request, and handles it.
// Returns number of items posted, or error
func (qh *ChannelQueueHandler) processOneRequest(prefix string, bucketOpts ...option.ClientOption) (int, error) {
	// Use proper storage bucket.
	parts, err := ParsePrefix(prefix)
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		metrics.FailCount.WithLabelValues("BadPrefix").Inc()
		return 0, err
	}
	bucketName := parts[1]
	bucket, err := GetBucket(bucketOpts, qh.Config.Project, bucketName, false)
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
	log.Println("Added ", n, prefix, " tasks to ", qh.Queue)
	return n, err
}

// handleLoop processes requests on input channel
func (qh *ChannelQueueHandler) handleLoop(next api.TaskPipe, bucketOpts ...option.ClientOption) {
	log.Println("Starting handler for", qh.Queue)
	// TODO - should we purge the queue here?
	qh.waitForEmptyQueue()
	for task := range qh.MsgChan {
		log.Println("Handling", task)
		task.Queue = qh.Queue
		task.Update(state.Queuing)

		n, err := qh.processOneRequest(task.Name, bucketOpts...)
		if err != nil && err != io.EOF {
			// TODO return error through Response()
			// Currently, processOneRequest logs error and increments metric.
			log.Println(err)
			task.SetError(err, "ProcessOneRequest")
			continue
		}
		if qh.TestMode {
			log.Println("test mode")
			n = 1
		}

		task.Update(state.Processing)

		if n > 0 {
			// Must wait for empty queue before proceeding with dedupping.
			// This ensures that the data has actually been processed, rather
			// than just sitting in the queue or in the pipeline.
			qh.waitForEmptyQueue()
			task.Queue = ""
			task.Update(state.Stabilizing)

			log.Println(qh.Queue, "sending", task.Name, "to dedup handler")
			if next != nil {
				// This may block if previous hasn't finished.  Should be rare.
				next.Sink() <- task
			} else {
				// TODO - or error?
				task.Queue = ""
				task.Update(state.Done)
			}
		} else {
			log.Println("No task files")
			task.Queue = ""
			task.Update(state.Done)
			task.Delete()
		}
	}
	log.Println(qh.Queue, "waiting for deduper to close")
	if next != nil {
		close(next.Sink())
		<-next.Response()
	}
	log.Println("Exiting handler for", qh.Queue)
	close(qh.ResponseChan)
}

// StartHandleLoop starts a go routine that waits for work on channel, and
// processes it.
// Returns a channel that closes when input channel is closed and final processing is complete.
func (qh *ChannelQueueHandler) StartHandleLoop(next api.TaskPipe, bucketOpts ...option.ClientOption) {
	go qh.handleLoop(next, bucketOpts...)
}

// NewChannelQueueHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewChannelQueueHandler(config cloud.Config, queue string, next api.TaskPipe, bucketOpts ...option.ClientOption) (*ChannelQueueHandler, error) {
	qh, err := NewQueueHandler(config.Client, config.Project, queue)
	if err != nil {
		return nil, err
	}
	msg := make(chan state.Task)
	rsp := make(chan error)
	cqh := ChannelQueueHandler{config, qh, msg, rsp}

	cqh.StartHandleLoop(next, bucketOpts...)
	return &cqh, nil
}
