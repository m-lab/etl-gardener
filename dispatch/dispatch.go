// Package dispatch contains the logic for dispatching new reprocessing tasks.

package dispatch

import (
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"time"
)

// DESIGN:
// There will be N channels that will handle posting to queue, waiting for queue drain,
// and deduplication and cleanup.
// The dispatch loop will simply step through days, submitting dates to the input queue.
// We don't care which queue a date goes to.  Only reason for multiple queues is to rate
// limit each individual day.

// Dispatcher globals
type Dispatcher struct {
	Queues []chan<- string
	Done   []<-chan bool

	StartDate time.Time
}

<<<<<<< HEAD
// NewDispatcher creates a proof of concept dispatcher
// hard coded to mlab-testing.  For testing / proof of concept only.
// TODO - replace with functional code.
func NewDispatcher() (*Dispatcher, error) {
	queues := make([]chan<- string, 0, 4)
	done := make([]<-chan bool, 0, 4)
	for i := 0; i < 4; i++ {
		q, d, err := NewChannelQueueHandler(http.DefaultClient, "mlab-testing",
			fmt.Sprintf("test-queue-%d", i))
=======
// NewDispatcher creates a dispatcher that will spread requests across multiple
// QueueHandlers.
func NewDispatcher(httpClient *http.Client, project, queueBase string, numQueues int, startDate time.Time) (*Dispatcher, error) {
	queues := make([]chan<- string, 0, numQueues)
	done := make([]<-chan bool, 0, numQueues)
	for i := 0; i < numQueues; i++ {
		q, d, err := NewChannelQueueHandler(httpClient, project,
			fmt.Sprintf("%s%d", queueBase, i))
>>>>>>> b63e48d... More realistic dispatcher
		if err != nil {
			return nil, err
		}
		queues = append(queues, q)
		done = append(done, d)
	}

	return &Dispatcher{Queues: queues, Done: done, StartDate: startDate}, nil
}

// Kill closes all the channels, and waits for all the dones.
func (disp *Dispatcher) Kill() {
	for i := range disp.Queues {
		close(disp.Queues[i])
	}
	// Wait for all of the done events.
	for i := range disp.Done {
		<-disp.Done[i]
	}
}

// Add will post the request to next available queue.
func (disp *Dispatcher) Add(prefix string) {
	// Easiest to do this on the fly, since it requires the prefix in the cases.
	cases := make([]reflect.SelectCase, 0, len(disp.Queues))
	for i := range disp.Queues {
		c := reflect.SelectCase{Dir: reflect.SelectSend,
			Chan: reflect.ValueOf(disp.Queues[i]), Send: reflect.ValueOf(prefix)}
		cases = append(cases, c)
	}
	reflect.Select(cases)
}

// DoDispatchLoop looks for next work to do.
// It should generally be blocked on the queues.
// TODO - Just for proof of concept. Replace with more useful code.
func (disp *Dispatcher) DoDispatchLoop() {
	for {
		// TODO - make this something real!
		disp.Add("gs://archive-mlab-sandbox/ndt/2017/09/24/")

		time.Sleep(time.Duration(30+rand.Intn(60)) * time.Second)
	}
}
