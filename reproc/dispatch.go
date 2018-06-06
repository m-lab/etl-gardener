// Package reproc handles the top level coordination of reprocessing tasks.
// Its primary responsibilities are:
//   1. Keep track of tasks in flight.
//   2. Create new tasks.
//   3. Allocate available task queues to new Tasks.
//   4. Maintain the top level persistent state as tasks are created and finished.
//   5. Provide status snapshots.
//   6. Coordinate termination.
package reproc

import (
	"io"
	"log"
	"sync"

	"github.com/m-lab/etl-gardener/state"
)

/*****************************************************************************/
/*                         Terminator helper struct                          */
/*****************************************************************************/

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

/*****************************************************************************/
/*                               TaskTracker                                 */
/*****************************************************************************/

// TaskTracker handles the top level Task coordination and persistence.
// It is a bit complicated because it is responsible for tracking
// multiple tasks, each with its own goroutine, status requests (coming from
// the http server), new task requests, and the termination signal.  It
// coordinates processing of these sources using selects on channels, avoiding
// other synchronization primitives, aside from a single WaitGroup used to
// track how many goroutines are in flight.
//
// TODO: Eventually, TaskTracker will also be responsible for persisting state
// changes to Datastore, through a StateSaver.
//
// All exported methods are safe to call from any goroutine.
// All non-exported methods should be called only from the single goroutine
// launched by RunHandler.
type TaskTracker struct {
	updates        chan<- state.Task    // Channel for submitting updates.
	statusRequests chan<- statusRequest // status request channel
	taskQueues     <-chan string        // Channel through which queues recycled.

	// For managing termination.
	*Terminator
}

type statusRequest struct {
	io.Writer
	done chan struct{}
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
	tt.runHandler(updates, taskQueues, status)
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

// handleUpdate processes Task update events, primarily to make Task status
// available for status requests.
func (tt *TaskTracker) handleUpdate(update state.Task, taskQueueChan chan<- string) {
	log.Println("Handling", update)
	// TODO
}

// runHandler starts a goroutine that handles ALL operations on the state object,
// which avoids worrying about concurrency.
// New Tasks and Task updates arrive on the updates channel.
// Status requests arrive on the status channel.
func (tt *TaskTracker) runHandler(updates <-chan state.Task, taskQueues chan<- string, status <-chan statusRequest) {
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
