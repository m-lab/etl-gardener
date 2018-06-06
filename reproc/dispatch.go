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
	"errors"
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
/*                               TaskHandler                                 */
/*****************************************************************************/

// TaskHandler handles the top level Task coordination.
// It is responsible for starting tasks, recycling queues, and handling the
// termination signal.
type TaskHandler struct {
	taskQueues chan string // Channel through which queues recycled.

	// For managing termination.
	*Terminator
}

// NewTaskHandler creates a new TaskHandler.
func NewTaskHandler(queues []string) *TaskHandler {
	// Create taskQueue channel, and preload with queues.
	taskQueues := make(chan string, len(queues))
	for _, q := range queues {
		taskQueues <- q
	}

	return &TaskHandler{taskQueues, NewTerminator()}
}

// Nop does nothing, but is used for coverage testing.
func nop() {}

// ErrTerminating is returned e.g. by AddTask, when tracker is terminating.
var ErrTerminating = errors.New("TaskHandler is terminating")

// AddTask adds a new task, blocking until the task has been accepted.
// This will typically be repeated called by another goroutine responsible
// for driving the reprocessing.
// May return ErrTerminating, if th has started termination.
func (th *TaskHandler) AddTask(prefix string) error {
	select {
	// Wait until there is an available task queue.
	case queue := <-th.taskQueues:
		t := state.Task{Name: prefix, Queue: queue, State: state.Initializing}

		// WARNING:  There is a race here when terminating, if a task gets
		// a queue here and calls Add().  This races with the thread that started
		// the termination and calls Wait().
		th.Add(1)
		go t.Process(th.taskQueues, th.Terminator)
		return nil

	// Or until we start termination.
	case <-th.GetNotifyChannel():
		// If we are terminating, do nothing.
		return ErrTerminating
	}
}
