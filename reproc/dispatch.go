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
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/metrics"

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
	once        sync.Once     // Protects the exit initiation.
	terminating chan struct{} // Channel to trigger termination.
	sync.WaitGroup
}

// GetNotifyChannel returns the channel that indicates termination has started.
func (t *Terminator) GetNotifyChannel() <-chan struct{} {
	return t.terminating
}

// Terminate initiates termination.  May be called multiple times.
func (t *Terminator) Terminate() {
	t.once.Do(func() {
		// we consumed the token, so close the channel.
		close(t.terminating)
	})
}

// NewTerminator creates a new Terminator (termination manager)
func NewTerminator() *Terminator {
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	return &Terminator{sync.Once{}, make(chan struct{}), sync.WaitGroup{}}
}

/*****************************************************************************/
/*                               TaskHandler                                 */
/*****************************************************************************/

// TaskHandler handles the top level Task coordination.
// It is responsible for starting tasks, recycling queues, and handling the
// termination signal.
type TaskHandler struct {
	expName    string                // The string used for task.Experiment, which is used in Datastore queries.
	exec       state.Executor        // Executor passed to new tasks
	taskQueues chan string           // Channel through which queues recycled.
	saver      state.PersistentStore // The Saver used to save task states.

	// For managing termination.
	*Terminator
}

// NewTaskHandler creates a new TaskHandler.
func NewTaskHandler(expKey string, exec state.Executor, queues []string, saver state.PersistentStore) *TaskHandler {
	// Create taskQueue channel, and preload with queues.
	taskQueues := make(chan string, len(queues))
	for _, q := range queues {
		taskQueues <- q
	}

	return &TaskHandler{expKey, exec, taskQueues, saver, NewTerminator()}
}

// ErrTerminating is returned e.g. by AddTask, when tracker is terminating.
var ErrTerminating = errors.New("TaskHandler is terminating")

// StartTask starts a single task.  It should be properly initialized except for saver.
// If the task had previously errored, this should clear the error from datastore.
func (th *TaskHandler) StartTask(ctx context.Context, t state.Task) {
	t.SetSaver(th.saver)

	// WARNING:  There is a race here when terminating, if a task gets
	// a queue here and calls Add().  This races with the thread that started
	// the termination and calls Wait().
	th.Add(1)

	// We pass a function to Process that it should call when finished
	// using the queue (when queue has drained.  Since this runs in its own
	// go routine, we need to avoid closing the taskQueues channel, which
	// could then cause panics.
	doneWithQueue := func() {
		log.Println("Returning", t.Queue)
		th.taskQueues <- t.Queue
	}
	go t.Process(ctx, th.exec, doneWithQueue, th.Terminator)
}

// This waits for a queue or termination notice.
func (th *TaskHandler) waitForQueue(ctx context.Context, t state.Task) (string, error) {
	select {
	// Wait until there is an available task queue.
	case queue := <-th.taskQueues:
		return queue, nil

	// Or until we start termination.
	case <-th.GetNotifyChannel():
		// If we are terminating, do nothing.
		return "", ErrTerminating
	}
}

// The sleeps for the requested time, but will break if the termination channel is closed.
func (th *TaskHandler) sleepWithTerminationCheck(delay time.Duration) error {
	// Check whether we are terminating.
	ticker := time.NewTicker(delay)
	select {
	case <-th.GetNotifyChannel():
		// If we are terminating, do nothing.
		ticker.Stop()
		return ErrTerminating
	case <-ticker.C:
		return nil
	}
}

func (th *TaskHandler) waitForPreviousTask(ctx context.Context, t state.Task) error {
	for {
		taskStatus, err := t.GetTaskStatus(ctx)
		if err != nil {
			log.Println(err)
			// TODO handle these errors properly.
			return err
		}
		switch {
		case taskStatus.State == state.Invalid || taskStatus.State == state.Done:
			return nil
		case taskStatus.ErrMsg != "":
			log.Printf("Restarting task that errored: %+v", taskStatus)
			return nil
		case time.Since(taskStatus.UpdateTime) > 12*time.Hour:
			log.Printf("Restarting task that has been idle more than 12 hour: %+v", taskStatus)
			return nil
		default:
		}

		log.Println("Delaying restart of", t.Name, "in state", state.StateNames[taskStatus.State])
		if err = th.sleepWithTerminationCheck(5 * time.Minute); err != nil {
			return err
		}
	}
}

// AddTask adds a new task, blocking until the task has been accepted.
// This will typically be repeated called by another goroutine responsible
// for driving the reprocessing.
// May return ErrTerminating, if th has started termination.
// TODO: Add prometheus metrics.
//
// More detail:
//   This call will block until two criteria are met:
//    1. The previous instance of the task must have completed, errored, or
//       been idle more than 12 hours.
//    2. A queue must be allocated.
//   No queue is allocated until #1 is satisfied.  While waiting for either condition,
//   the termination notice is also respected, causing ErrTerminating to be returned.
func (th *TaskHandler) AddTask(ctx context.Context, prefix string) error {
	log.Println("Waiting for a queue")
	t, err := state.NewTask(th.expName, prefix, "No queue yet", th.saver)
	if err != nil {
		log.Println(err)
		return err
	}

	// Wait until the previous instance of the task has completed,
	// errored, or been idle for 24 hours.
	if err = th.waitForPreviousTask(ctx, *t); err != nil {
		return err
	}

	// Now wait for a queue and start the task.
	queue, err := th.waitForQueue(ctx, *t)
	if err == nil {
		t.Queue = queue
		log.Println("Adding:", t.Name)
		th.StartTask(ctx, *t)
	}
	return err
}

// RestartTasks restarts all the tasks, allocating queues as needed.
// SHOULD ONLY be called at startup.
// Returns date of next jobs to process.
// NOTE: The ctx parameter will be used for all rpc operations for all tasks, and must be
// long lived.
func (th *TaskHandler) RestartTasks(ctx context.Context, tasks []state.Task) (time.Time, error) {
	// Retrieve all task queues from the pool.
	queues := make(map[string]struct{}, 20)
queueLoop:
	for {
		select {
		case q := <-th.taskQueues:
			queues[q] = struct{}{}
		default:
			break queueLoop
		}
	}

	// Restart all tasks, allocating original queue as required.
	// Keep track of latest date seen.
	maxDate := time.Time{}
	for i := range tasks {
		t := tasks[i]
		if t.ErrInfo != "" || t.ErrMsg != "" {
			log.Println("Skipping:", t.Name, t.ErrMsg, t.ErrInfo)
			metrics.FailCount.WithLabelValues("skipping task with error").Inc()
			continue
		}
		if t.Queue != "" {
			if strings.TrimSpace(t.Queue) != t.Queue {
				log.Println("invalid queue name", t)
				metrics.FailCount.WithLabelValues("bad queue name").Inc()
				// Skip updating task date, as this entry is somehow corrupted.
				continue
			}
			_, ok := queues[t.Queue]
			if ok {
				delete(queues, t.Queue)
				log.Println("Restarting", t)
				th.StartTask(ctx, t)
			} else {
				log.Println("Queue", t.Queue, "already in use.  Skipping", t)
				metrics.FailCount.WithLabelValues("queue not available").Inc()
				continue
			}
		} else {
			// No queue, just restart...
			log.Println("Restarting", t)
			th.StartTask(ctx, t)
		}
		if t.Date.After(maxDate) {
			maxDate = t.Date
		}
	}
	log.Println("Max date found:", maxDate)

	// Return the unused queues to the pool.
	for q := range queues {
		th.taskQueues <- q
	}

	return maxDate, nil
}
