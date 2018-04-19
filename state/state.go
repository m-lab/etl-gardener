// Package state handles persistent state required across instances.
package state

// NOTE: Avoid dependencies on any other m-lab code.
import (
	"errors"
)

// State indicates the state of a single Task in flight.
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

// Saver provides API for saving Task state.
type Saver interface {
	SaveTask(t Task) error
	DeleteTask(t Task) error
}

// DatastoreSaver will implement a Saver that stores Task state in DataStore.
type DatastoreSaver struct {
	// TODO - add datastore stuff
}

// SaveTask implements Saver.SaveTask using Datastore.
func (s *DatastoreSaver) SaveTask(t Task) error { return nil }

// DeleteTask implements Saver.DeleteTask using Datastore.
func (s *DatastoreSaver) DeleteTask(t Task) error { return nil }

// Task contains the state of a single Task.
// These will be stored and retrieved from DataStore
type Task struct {
	Name    string // e.g. gs://archive-mlab-oti/ndt/2017/06/01/
	Suffix  string // e.g. 20170601
	State   State
	Queue   string // The queue the tasks were submitted to, or empty.
	JobID   string // JobID, when the state is Deduplicating
	Err     error  // Processing error, if any
	ErrInfo string // More context about any error, if any

	saver Saver // Saver is used for Save operations. Stored locally, but not persisted.
}

// ErrNoSaver is returned when saver has not been set.
var ErrNoSaver = errors.New("Task.saver is nil")

// Save saves the task state to the "saver".
func (t *Task) Save() error {
	if t.saver == nil {
		return ErrNoSaver
	}
	return t.saver.SaveTask(*t)
}

// Update updates the task state, and saves to the "saver".
func (t *Task) Update(st State) error {
	if t.saver == nil {
		return ErrNoSaver
	}
	t.State = st
	return t.saver.SaveTask(*t)
}

// Delete removes by calling saver.DeleteTask.
func (t *Task) Delete() error {
	if t.saver == nil {
		return ErrNoSaver
	}
	return t.saver.DeleteTask(*t)
}

// SetError adds error information and saves to the "saver"
func (t *Task) SetError(err error, info string) error {
	if t.saver == nil {
		return ErrNoSaver
	}
	t.Err = err
	t.ErrInfo = info
	return t.saver.SaveTask(*t)
}

// SetSaver sets the value of the saver to be used for all other calls.
func (t *Task) SetSaver(saver Saver) {
	t.saver = saver
}
