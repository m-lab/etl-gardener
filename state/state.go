// Package state handles persistent state required across instances.
package state

// NOTE: Avoid dependencies on any other m-lab code.
import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/datastore"
)

// State indicates the state of a single Task in flight.
type State int

// State definitions
const (
	Invalid       State = iota
	Initializing        // Task is being initialized.
	Queuing             // Queuing all task files for the task (date/experiment).
	Processing          // Done queuing, waiting for the queue to empty.
	Stabilizing         // Waiting for streaming buffer to be empty
	Deduplicating       // Bigquery deduplication query in process.
	Finishing           // Deleting template table, copying to final table, deleting partition.
	Done                // Done all processing, ok to delete state.
)

// StateNames maps from State to string, for use in String()
var StateNames = map[State]string{
	Initializing:  "Initializing",
	Queuing:       "Queuing",
	Processing:    "Processing",
	Stabilizing:   "Stabilizing",
	Deduplicating: "Deduplicating",
	Finishing:     "Finishing",
	Done:          "Done",
}

// Saver provides API for saving Task state.
type Saver interface {
	SaveTask(t Task) error
	DeleteTask(t Task) error
}

// DatastoreSaver will implement a Saver that stores Task state in Datastore.
type DatastoreSaver struct {
	Client    *datastore.Client
	Namespace string
}

// NewDatastoreSaver creates and returns an appropriate saver.
func NewDatastoreSaver() (*DatastoreSaver, error) {
	project := os.Getenv("PROJECT")
	client, err := datastore.NewClient(context.Background(), project)
	if err != nil {
		return nil, err
	}
	return &DatastoreSaver{client, "gardener"}, nil
}

// SaveTask implements Saver.SaveTask using Datastore.
// TODO - do we want to use transactions and some consistency checking?
func (ds *DatastoreSaver) SaveTask(t Task) error {
	k := datastore.NameKey("task", t.Name, nil)
	k.Namespace = ds.Namespace
	_, err := ds.Client.Put(context.Background(), k, &t)
	return err
}

// DeleteTask implements Saver.DeleteTask using Datastore.
func (ds *DatastoreSaver) DeleteTask(t Task) error {
	k := datastore.NameKey("task", t.Name, nil)
	k.Namespace = ds.Namespace
	return ds.Client.Delete(context.Background(), k)
}

// Task contains the state of a single Task.
// These will be stored and retrieved from DataStore
type Task struct {
	Name        string // e.g. gs://archive-mlab-oti/ndt/2017/06/01/
	TableSuffix string // e.g. 20170601
	State       State
	Queue       string // The queue the task files are submitted to, or "".
	JobID       string // BigQuery JobID, when the state is Deduplicating
	ErrMsg      string // Task handling error, if any
	ErrInfo     string // More context about any error, if any

	saver Saver // Saver is used for Save operations. Stored locally, but not persisted.
}

func (t Task) String() string {
	return fmt.Sprintf("{%s: %s, Q:%s, J:%s, E:%s (%s)}", t.Name, StateNames[t.State], t.Queue, t.JobID, t.ErrMsg, t.ErrInfo)
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
	t.ErrMsg = err.Error()
	t.ErrInfo = info
	return t.saver.SaveTask(*t)
}

// SetSaver sets the value of the saver to be used for all other calls.
func (t *Task) SetSaver(saver Saver) {
	t.saver = saver
}

// Terminator interface provides notification and synchronization for termination.
type Terminator interface {
	GetNotifyChannel() <-chan struct{}
	Terminate()
	Add(n int)
	Done()
	Wait()
}

// Nop does nothing, but is used for coverage testing.
func nop() {}

// Process handles all steps of processing a task.
func (t *Task) Process(tq chan<- string, term Terminator) {
	select {
	case <-time.After(time.Duration(1+rand.Intn(10)) * time.Millisecond):
		tq <- t.Queue
		// Wait until one of these...
		select {
		case <-time.After(time.Duration(1+rand.Intn(10)) * time.Millisecond):
			nop()
		case <-term.GetNotifyChannel():
			nop()
		}
	case <-term.GetNotifyChannel():
		nop()
	}
	term.Done()
}
