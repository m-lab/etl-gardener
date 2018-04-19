// Package dispatch contains the logic for dispatching new reprocessing tasks.
package dispatch

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/state"

	"github.com/m-lab/etl-gardener/api"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"google.golang.org/api/option"
)

// DESIGN:
// There will be N channels that will handle posting to queue, waiting for queue drain,
// and deduplication and cleanup.
// The dispatch loop will simply step through days, submitting dates to the input queue.
// We don't care which queue a date goes to.  Only reason for multiple queues is to rate
// limit each individual day.

// Dispatcher globals
type Dispatcher struct {
	Handlers    []api.BasicPipe
	StartDate   time.Time
	Terminating bool // Indicates when Terminate has been called.
	Saver       state.Saver
	lock        sync.Mutex
}

// Dispatcher related errors.
var (
	ErrTerminating = errors.New("dispatcher is terminating")
)

// NewDispatcher creates a dispatcher that will spread requests across multiple
// QueueHandlers.
// bucketOpts may be used to provide a fake client for bucket operations.
func NewDispatcher(httpClient *http.Client, project, queueBase string, numQueues int,
	startDate time.Time, saver state.Saver, bucketOpts ...option.ClientOption) (*Dispatcher, error) {

	handlers := make([]api.BasicPipe, 0, numQueues)
	for i := 0; i < numQueues; i++ {
		queue := fmt.Sprintf("%s%d", queueBase, i)
		// First build the dedup handler.
		dedup := NewDedupHandler()
		// Build QueueHandler that chains to dedup handler.

		cqh, err := tq.NewChannelQueueHandler(httpClient, project,
			queue, dedup, bucketOpts...)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, cqh)
	}

	return &Dispatcher{Handlers: handlers, StartDate: startDate, Terminating: false, Saver: saver}, nil
}

// Terminate closes all the channels, and waits for all the dones.
func (disp *Dispatcher) Terminate() {
	disp.lock.Lock()
	defer disp.lock.Unlock()
	if disp.Terminating {
		return
	}
	disp.Terminating = true
	for i := range disp.Handlers {
		close(disp.Handlers[i].Sink())
	}
	// Wait for all of the done events.
	// TODO - for now, no errors coming back, so we only see the close.
	for i := range disp.Handlers {
		<-disp.Handlers[i].Response()
	}
}

// Add will post the request to next available queue.
// May return ErrTerminating if Terminate has been called.
func (disp *Dispatcher) Add(prefix string) error {
	disp.lock.Lock()
	defer disp.lock.Unlock()
	if disp.Terminating {
		return ErrTerminating
	}
	// TODO - create Task entry in persistent store, in Initializing state.
	task := state.Task{Name: prefix, State: state.Initializing}
	task.SetSaver(disp.Saver)
	err := disp.Saver.SaveTask(task)
	if err != nil {

	}

	// Easiest to do this on the fly, since it requires the prefix in the cases.
	cases := make([]reflect.SelectCase, 0, len(disp.Handlers))
	for i := range disp.Handlers {
		c := reflect.SelectCase{Dir: reflect.SelectSend,
			Chan: reflect.ValueOf(disp.Handlers[i].Sink()), Send: reflect.ValueOf(task)}
		cases = append(cases, c)
	}
	log.Println("Waiting for empty queue for", prefix)
	reflect.Select(cases)
	return nil
}

// DoDispatchLoop just sequences through archives in date order.
// It will generally be blocked on the queues.
func (disp *Dispatcher) DoDispatchLoop(bucket string, experiments []string) {
	next := disp.StartDate

	for {
		for _, e := range experiments {
			prefix := next.Format(fmt.Sprintf("gs://%s/%s/2006/01/02/", bucket, e))
			disp.Add(prefix)
		}

		next = next.AddDate(0, 0, 1)

		// If gardener has processed all dates up to two days ago,
		// start over.
		if next.Add(48 * time.Hour).After(time.Now()) {
			// TODO - load this from DataStore
			next = disp.StartDate
		}
	}
}
