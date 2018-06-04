// Package reproc handles the top level coordination of reprocessing tasks.
// Its primary responsibilities are:
//   1. Keep track of tasks in flight.
//   2. Allocate task queues.
//   3. Create new tasks.
//   4. Maintain the persistent state as tasks are created and finished.
//   5. Provide status snapshots.
//
// A separate Task (??) package provides the detailed processing of individual tasks.
// TODO make this the dispatch package?
package reproc

import (
	"flag"
	"io"
	"log"
	"sync"

	"github.com/m-lab/etl-gardener/state"
)

// Environment provides "global" variables.
type environment struct {
	TestMode bool
}

// env provides environment vars.
var env environment

func init() {
	// HACK This allows some modified behavior when running unit tests.
	if flag.Lookup("test.v") != nil {
		env.TestMode = true
	}
}

// Terminator provides signals for a termination process.
//  GetNotifyChannel to get the channel that goroutines should monitor.
//     Whenever caller creates a goroutine, it should first call Add(1).
//  GetNotifyChannel() to get termination notifier channel to pass to
//     goroutines (after calling Add(1))
//  Terminate() to start the termination process.
//  Call Done() whenever a goroutine completes.
//  Call Wait() to wait until all goroutines have completed.
type Terminator struct {
	semaphore   chan struct{} // Protects the exit initiation.
	terminating chan struct{} // Channel to trigger termination.
	sync.WaitGroup
}

// GetNotifyChannel returns the channel that indicates termination has started.
func (t *Terminator) GetNotifyChannel() <-chan struct{} {
	return t.terminating
}

// Terminate initiates termination.  May be called multiple times.
func (t *Terminator) Terminate() {
	select {
	case <-t.semaphore:
		// Consumed the semaphore, so close the channel.
		close(t.terminating)
	default:
		// If semaphore already consumed, do nothing.
	}
}

// NewTerminator creates a new Terminator (termination manager)
func NewTerminator() *Terminator {
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	return &Terminator{sem, make(chan struct{}), sync.WaitGroup{}}
}

type statusRequest struct {
	io.Writer
	done chan struct{}
}

// TaskTracker handles the top level Task coordination and persistence.
// All exported methods are safe to call from any goroutine.
// All non-exported methods should be called only from the single goroutine
// executing Handle
type TaskTracker struct {
	updates        chan<- state.Task    // Channel for submitting updates.
	statusRequests chan<- statusRequest // status request channel
	taskQueues     <-chan string        // Channel through which queues recycled.

	// For managing termination.
	*Terminator
}

// StartTaskTracker creates and starts processing a TaskTracker.
func StartTaskTracker(queues []string) (*TaskTracker, error) {
	updates := make(chan state.Task, 0)
	status := make(chan statusRequest)

	// Create taskQueue channel, and preload with queues.
	taskQueues := make(chan string, len(queues))
	for _, q := range queues {
		taskQueues <- q
	}

	tt := TaskTracker{updates, status, taskQueues, NewTerminator()}
	tt.RunHandler(updates, taskQueues, status)
	return &tt, nil
}

// AddTask adds a new task, blocking until the task has been accepted.
func (tt *TaskTracker) AddTask(prefix string) {
	notify := tt.GetNotifyChannel()
	select {
	// Wait until there is an available task queue.
	case queue := <-tt.taskQueues:
		t := state.Task{Name: prefix, Queue: queue, State: state.Initializing}
		tt.updates <- t

	// Or until we start termination.
	case <-notify:
		// If we are terminating, do nothing.
	}
}

// GetStatus sends request to Handler, and blocks until it gets response.
func (tt *TaskTracker) GetStatus(w io.Writer) {
	req := statusRequest{w, make(chan struct{})}
	notify := tt.GetNotifyChannel()
	select {
	case tt.statusRequests <- req:
		<-req.done
	case <-notify:
		// If terminating, then just return.
		log.Println("Abandoning status request") // So we can see that this isn't tested.
	}
}

/**************************************************************************/
/*                    SINGLE THREADED Handler methods                     */
/**************************************************************************/
// status processes a status request in the handler thread.
func (tt *TaskTracker) status(request statusRequest) {
	// TODO
	request.Writer.Write([]byte("status..."))

	close(request.done)
}

// handleUpdate processes state update requests, including updating persistent
// storage.
func (tt *TaskTracker) handleUpdate(update state.Task, taskQueueChan chan<- string) {
	log.Println("Handling", update)
	// TODO
}

// RunHandler starts a goroutine that handles ALL operations on the state object,
// which avoids worrying about concurrency.
// New Tasks and Task updates arrive on the updates channel.
// Status requests arrive on the status channel.
func (tt *TaskTracker) RunHandler(updates <-chan state.Task, taskQueues chan<- string, status <-chan statusRequest) {
	tt.Add(1) // Reservation for this goroutine, outside goroutine to avoid race.
	exit := tt.GetNotifyChannel()
	go func() {
		for {
			select {
			case update := <-updates:
				tt.handleUpdate(update, taskQueues)
				continue
			case req := <-status:
				tt.status(req)
				continue
			case <-exit:
				log.Println("Detected termination")
				tt.Done() // Release reservation for this goroutine.
				return
			}
		}
	}()
}
