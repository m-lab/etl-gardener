// Package dispatch contains the logic for dispatching new reprocessing tasks.

package dispatch

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"
)

// DESIGN:
// There will be N channels that will handle posting to queue, waiting for queue drain,
// and deduplication and cleanup.
// The dispatch loop will simply step through days, submitting dates to the input queue.
// We don't care which queue a date goes to.  Only reason for multiple queues is to rate
// limit each individual day.

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(ErrorCount)
	prometheus.MustRegister(WarningCount)
}

var (
	// ErrorCount counts non-fatal errors.
	//
	// Provides metrics:
	//   etl_error_count{status}
	// Example usage:
	// metrics.ErrorCount.WithLabelValues("ok").Inc()
	ErrorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_error_count",
			Help: "Number of non-fatal errors.",
		},
		[]string{"status"},
	)

	// WarningCount counts the number of files processed with each status.
	//
	// Provides metrics:
	//   etl_fail_count{status}
	// Example usage:
	// metrics.WarningCount.WithLabelValues("funny xyz").Inc()
	WarningCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_warning_count",
			Help: "Number of processing warnings.",
		},
		[]string{"status"},
	)

	// FailCount counts the number of files processed with each status.
	//
	// Provides metrics:
	//   etl_fail_count{status}
	// Example usage:
	// metrics.FailCount.WithLabelValues("BadTableName").Inc()
	FailCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_fail_count",
			Help: "Number of processing failures.",
		},
		[]string{"status"},
	)
)

// Dispatcher globals
type Dispatcher struct {
	Queues      []chan<- string
	Done        []<-chan bool
	StartDate   time.Time
	Terminating bool // Indicates when Terminate has been called.
}

// Dispatcher related errors.
var ErrTerminating = errors.New("dispatcher is terminating")

// NewDispatcher creates a dispatcher that will spread requests across multiple
// QueueHandlers.
// bucketOpts may be used to provide a fake client for bucket operations.
func NewDispatcher(httpClient *http.Client, project, queueBase string, numQueues int, startDate time.Time, bucketOpts ...option.ClientOption) (*Dispatcher, error) {
	queues := make([]chan<- string, 0, numQueues)
	done := make([]<-chan bool, 0, numQueues)
	for i := 0; i < numQueues; i++ {
		q, d, err := NewChannelQueueHandler(httpClient, project,
			fmt.Sprintf("%s%d", queueBase, i), bucketOpts...)
		if err != nil {
			return nil, err
		}
		queues = append(queues, q)
		done = append(done, d)
	}

	return &Dispatcher{Queues: queues, Done: done, StartDate: startDate, Terminating: false}, nil
}

// Terminate closes all the channels, and waits for all the dones.
func (disp *Dispatcher) Terminate() {
	if disp.Terminating {
		return
	}
	disp.Terminating = true
	for i := range disp.Queues {
		close(disp.Queues[i])
	}
	// Wait for all of the done events.
	for i := range disp.Done {
		<-disp.Done[i]
	}
}

// Add will post the request to next available queue.
// May return ErrTerminating if Terminate has been called.
func (disp *Dispatcher) Add(prefix string) error {
	if disp.Terminating {
		return ErrTerminating
	}
	// Easiest to do this on the fly, since it requires the prefix in the cases.
	cases := make([]reflect.SelectCase, 0, len(disp.Queues))
	for i := range disp.Queues {
		c := reflect.SelectCase{Dir: reflect.SelectSend,
			Chan: reflect.ValueOf(disp.Queues[i]), Send: reflect.ValueOf(prefix)}
		cases = append(cases, c)
	}
	log.Println("Waiting for empty queue for", prefix)
	reflect.Select(cases)
	return nil
}

// DoDispatchLoop looks for next work to do.
// It should generally be blocked on the queues.
func (disp *Dispatcher) DoDispatchLoop(bucket string, experiments []string) {
	next := disp.StartDate

	for {
		for _, e := range experiments {
			prefix := next.Format(fmt.Sprintf("gs://%s/%s/2006/01/02/", bucket, e))
			disp.Add(prefix)
		}

		next = next.AddDate(0, 0, 1)
		// When we catch up to two days ago, start over.
		if next.Add(48 * time.Hour).After(time.Now()) {
			// TODO - load this from DataStore
			next = disp.StartDate
		}
	}
}
