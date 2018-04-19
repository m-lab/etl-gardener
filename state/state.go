// Package state handles persistent state required across instances
package state

// TODO - work out how to manage queues and new tasks.
// Some simplificaitons:
// 1. available queues should come from config, not state.
// 2. Tasks contain all info about queues in use.
// 3. Don't need to list the tasks in the persistent state.
// 4. The local state needs to cache quite a lot of this though.

import (
	"errors"
	"io"
	"time"
)

// High level design:
//  SystemState holds the dispatcher state, and has a GoRoutine that handles
//  state changes communicated to it through a single channel.
//
//  Task objects hold the state for individual taskes that are in flight.
//  Each task is initially allocated a Queue to host that task, and the
//  queue is returned to the SystemState when the task no longer needs it.

// State is a simple enum to indicate the state of a single Task/Task in flight.
type State int

// State definitions
const (
	Initializing  State = iota
	Queuing             // Queuing all tasks for the task.
	Processing          // Done queuing, waiting for the queue to empty.
	Stabilizing         // Waiting for streaming buffer to be empty
	Deduplicating       // Bigquery deduplication query in process.
	Finishing           // Deleting template table, copying to final table, deleting partition.
	Done                // Done all processing, ok to delete state.
)

// Loader provides API for persisting and retrieving state.
type Loader interface {
	LoadTask(name string) (*Task, error)
	LoadAllTasks(name string) ([]Task, error)

	LoadSystemState(ss *SystemState) error
}

// Saver provides API for saving Task state.
type Saver interface {
	SaveTask(t Task) error

	SaveSystem(s *SystemState) error
}

// Helpers contains all the helpers required when running a Task.
type Helpers struct {
	saver     Saver
	updater   chan<- Task
	terminate <-chan struct{}
}

// Task contains the full state of a particular queue.
// Given a DateState, we can determine what the most recent action was,
// what event to wait for, and what to do next.
type Task struct {
	Name    string // e.g. ndt/2017/06/01/
	Suffix  string // e.g. 20170601
	State   State
	Queue   string // The queue the tasks were submitted to, or empty.
	JobID   string // JobID, when the state is Deduplicating
	Err     error  // standard error, if any
	ErrInfo string // More context about any error, if any

	saver Saver
}

var ErrNoSaver = errors.New("Task.saver is nil")

func (t *Task) Save() error {
	if t.saver == nil {
		return ErrNoSaver
	}

	return t.saver.SaveTask(*t)
}

func (t *Task) SetSaver(saver Saver) {
	t.saver = saver
}

// SystemState holds the high level state of the reprocessing dispatcher.
// Note that the DataStore representation will have separate entries for
// each map entry.
type SystemState struct {
	NextDate time.Time           // The date about to be queued, or to be queued next.
	AllTasks map[string]struct{} // set of names of all tasks in flight.
}

// Thread safety invariants for SystemState
//  1. Only MonitorTasks updates the AllTasks/allTasks maps.
//  2. All accesses to NextDate and AllTasks are protected with Mutex "lock"
//  3. Updates to persistent store are lazy for Task removals.  AllTask inserts
//      and NextDate updates are always pushed to Saver before launching task. ????

// Describe writes a description of the complete state to w
func (ss *SystemState) Describe(w *io.Writer) {

}

// Add adds a single task.  Msg back to changeChan will update local state and DS
func (ss *SystemState) Add(t Task) {
}
