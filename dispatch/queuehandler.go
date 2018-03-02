package dispatch

import (
	"errors"
	"log"
	"net/http"
	"regexp"

	"github.com/m-lab/etl-gardener/cloud/tq"
)

// ChannelQueueHandler is an autonomous queue handler running in a go
// routine, fed by a channel.
type ChannelQueueHandler struct {
	*tq.QueueHandler
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

// StartHandleLoop starts a go routine that waits for work on channel, and
// processes it.  Returns a channel that will send true when input channel is closed.
func (chq *ChannelQueueHandler) StartHandleLoop() <-chan bool {
	done := make(chan bool)
	go func() {
		for {
			// TODO - should block here until queue is empty
			prefix, more := <-chq.Channel
			if more {
				// Use proper storage bucket.
				parts, err := parsePrefix(prefix)
				if err != nil {
					log.Println(err)
					continue
				}
				bucketName := parts[1]
				bucket, err := tq.GetBucket(nil, chq.Project, bucketName, false)
				//bucket, err := chq.getBucket(bucketName)
				if err != nil {
					log.Println(err)
					continue
				}
				log.Println(parts)
				chq.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
			} else {
				done <- true
				break
			}
		}
	}()
	return done
}

// NewChannelQueueHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewChannelQueueHandler(httpClient *http.Client, project, queue string) (chan<- string, <-chan bool, error) {
	qh, err := tq.NewQueueHandler(httpClient, project, queue)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan string)
	cqh := ChannelQueueHandler{qh, ch}

	done := cqh.StartHandleLoop()
	return ch, done, nil
}
