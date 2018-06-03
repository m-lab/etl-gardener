// Package state handles persistent state required across instances.
package state

// NOTE: Avoid dependencies on any other m-lab code.
import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/bqext"
)

// Environment provides "global" variables.
type environment struct {
	Project string
}

// env provides environment vars.
var env environment

func init() {
	env.Project = os.Getenv("PROJECT")
}

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
	Invalid:       "Invalid",
	Initializing:  "Initializing",
	Queuing:       "Queuing",
	Processing:    "Processing",
	Stabilizing:   "Stabilizing",
	Deduplicating: "Deduplicating",
	Finishing:     "Finishing",
	Done:          "Done",
}

// Task Errors
var (
	ErrInvalidQueue  = errors.New("invalid queue")
	ErrTaskSuspended = errors.New("task suspended")
)

// Executor describes an object that can do all the required steps to execute a Task.
// Used for mocking.
// Interface must update the task in place, so that state changes are all visible.
type Executor interface {
	// Advance to the next state.
	AdvanceState(task *Task)
	// Advance to the next state.
	DoAction(task *Task, terminate <-chan struct{})
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
	client, err := datastore.NewClient(context.Background(), env.Project)
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
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := ds.Client.Put(ctx, k, &t)
	if err != nil {
		return err
	}
	return ctx.Err()
}

// DeleteTask implements Saver.DeleteTask using Datastore.
func (ds *DatastoreSaver) DeleteTask(t Task) error {
	k := datastore.NameKey("task", t.Name, nil)
	k.Namespace = ds.Namespace
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := ds.Client.Delete(ctx, k)
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Helpers contains all the helpers required when running a Task.
type Helpers struct {
	ex            Executor
	UpdaterChan   chan<- Task
	TerminateChan <-chan struct{}
}

// NewHelpers returns a new Helpers object.
func NewHelpers(ex Executor, updaterChan chan<- Task, terminatorChan <-chan struct{}) Helpers {
	return Helpers{ex, updaterChan, terminatorChan}
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

	UpdateTime time.Time `datastore:",noindex"`

	err   error // internal error representation.  Not persisted.
	saver Saver // Saver is used for Save, Update, Delete, SetError operations.  Stored locally, but not persisted.
}

// NewTask creates a new task, which will use the provided Saver for persistence operations.
func NewTask(name string, saver Saver) Task {
	return Task{Name: name, saver: saver}
}

// HACK - remove from tq package?
const start = `^gs://(?P<bucket>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	prefixPattern = regexp.MustCompile(start + // #1 #2
		datePath) // #3 - YYYY/MM/DD
)

// ParsePrefix Parses prefix, returning {bucket, experiment, date string}, error
func (t *Task) ParsePrefix() ([]string, error) {
	fields := prefixPattern.FindStringSubmatch(t.Name)

	if fields == nil {
		return nil, errors.New("Invalid test path: " + t.Name)
	}
	if len(fields) < 4 {
		return nil, errors.New("Path does not include all fields: " + t.Name)
	}
	return fields, nil
}

// SourceAndDest creates BQ Table entities for the source templated table, and destination partition.
func (t *Task) SourceAndDest(ds *bqext.Dataset) (*bigquery.Table, *bigquery.Table, error) {
	// Launch the dedup request, and save the JobID
	parts, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		metrics.FailCount.WithLabelValues("BadDedupPrefix")
		return nil, nil, err
	}

	src := ds.Table(parts[2] + "_" + strings.Join(strings.Split(parts[3], "/"), ""))
	dest := ds.Table(parts[2] + "$" + strings.Join(strings.Split(parts[3], "/"), ""))
	return src, dest, nil
}

func (t Task) String() string {
	return fmt.Sprintf("{%s: %s, Q:%s, J:%s, E:%v (%s)}", t.Name, StateNames[t.State], t.Queue, t.JobID, t.err, t.ErrInfo)
}

// ErrNoSaver is returned when saver has not been set.
var ErrNoSaver = errors.New("Task.saver is nil")

// Save saves the task state to the "saver".
func (t *Task) Save() error {
	if t.saver == nil {
		return ErrNoSaver
	}
	t.UpdateTime = time.Now()
	return t.saver.SaveTask(*t)
}

// Update updates the task state, and saves to the "saver".
func (t *Task) Update(st State) error {
	if t.saver == nil {
		return ErrNoSaver
	}
	t.State = st
	t.UpdateTime = time.Now()
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
	t.UpdateTime = time.Now()
	return t.saver.SaveTask(*t)
}

// HasError returns true if the task has encountered an error.
func (t *Task) HasError() bool {
	return t.err != nil || t.ErrMsg != ""
}

// IsSuspended indicates whether the task is suspended due to termination
func (t *Task) IsSuspended() bool {
	return t.err == ErrTaskSuspended || t.ErrMsg == ErrTaskSuspended.Error()
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
// TODO: remove this once there is actual implementation in Process.
func nop() {}

// Process handles all steps of processing a task.
// TODO: Real implementation. This is a dummy implementation, to support testing TaskHandler.
func (t Task) Process(ex Executor, tq chan<- string, term Terminator) {
	for t.State != Done && t.err == nil {
		select {
		case <-term.GetNotifyChannel():
			t.SetError(ErrTaskSuspended, "Terminating")
			return
		default:
			switch t.State {
			case Initializing:
				tq <- t.Queue

			default:
				ex.DoAction(&t, term.GetNotifyChannel())
				ex.AdvanceState(&t)
			}
		}
	}
	term.Done()
}

// GetStatus fetches all Task state from Datastore.
func (ds *DatastoreSaver) GetStatus(ctx context.Context) ([]Task, error) {
	q := datastore.NewQuery("task").Namespace(ds.Namespace)
	tasks := make([]Task, 0, 100)
	_, err := ds.Client.GetAll(ctx, q, &tasks)
	if err != nil {
		return nil, err
		// Handle error.
	}
	return tasks, nil
}

// WriteHTMLStatusTo writes HTML formatted task status.
func WriteHTMLStatusTo(w io.Writer) error {
	ds, err := NewDatastoreSaver()
	if err != nil {
		fmt.Fprintln(w, "Error creating Datastore client:", err)
		return err
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tasks, err := ds.GetStatus(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	if ctx.Err() != nil {
		return err
	}
	fmt.Fprintf(w, "<div>\nTask State</br>\n")
	for i := range tasks {
		fmt.Fprintf(w, "%s</br>\n", tasks[i])
	}
	fmt.Fprintf(w, "</div>\n")
	return nil
}
