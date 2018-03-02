// Package dispatch contains the logic for dispatching new reprocessing tasks.

package dispatch

import (
	"fmt"
	"log"
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

// HACK
func NewDispatcher() (*Dispatcher, error) {
	queues := make([]chan<- string, 0, 4)
	done := make([]<-chan bool, 0, 4)
	for i := 0; i < 4; i++ {
		q, d, err := NewChannelQueueHandler(http.DefaultClient, "mlab-testing",
			fmt.Sprintf("test-queue-%d", i))
		if err != nil {
			return nil, err
		}
		queues = append(queues, q)
		done = append(done, d)
	}

	return &Dispatcher{queues, done,
		time.Now().AddDate(0, 0, -10)}, nil
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
		cases = append(cases, reflect.SelectCase{reflect.SelectSend,
			reflect.ValueOf(disp.Queues[i]), reflect.ValueOf(prefix)})
	}
	reflect.Select(cases)
}

// DoDispatchLoop looks for next work to do.
// It should generally be blocked on the queues.
// TODO - Just for proof of concept. Replace with more useful code.
func (disp *Dispatcher) DoDispatchLoop() {
	for {
		log.Println("Dispatch Loop")
		// TODO - add content.

		disp.Add("gs://bucket/foobar/2017/01/02/")

		time.Sleep(time.Duration(10+rand.Intn(20)) * time.Second)
	}
}
