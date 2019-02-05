// Package state handles persistent state required across instances.
package state

// NOTE: Avoid dependencies on any other m-lab code.
import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/go/dataset"
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
	ErrInvalidQueue           = errors.New("invalid queue")
	ErrTaskSuspended          = errors.New("task suspended")
	ErrTableNotFound          = errors.New("Not found: Table")
	ErrRowsFromOtherPartition = errors.New("Rows belong to different partition")
)

// Executor describes an object that can do all the required steps to execute a Task.
// Used for mocking.
// Interface must update the task in place, so that state changes are all visible.
type Executor interface {
	Next(ctx context.Context, task *Task, terminate <-chan struct{}) error
}

// Saver provides API for saving Task state.
type Saver interface {
	SaveTask(ctx context.Context, t Task) error
	DeleteTask(ctx context.Context, t Task) error
}

// DatastoreSaver will implement a Saver that stores Task state in Datastore.
type DatastoreSaver struct {
	Client    *datastore.Client
	Namespace string
}

// NewDatastoreSaver creates and returns an appropriate saver.
// ctx is only used to create the client.
// TODO - if this ever needs more context, use cloud.Config
func NewDatastoreSaver(ctx context.Context, project string) (*DatastoreSaver, error) {
	client, err := datastore.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	return &DatastoreSaver{client, "gardener"}, nil
}

// SaveTask implements Saver.SaveTask using Datastore.
// TODO - do we want to use transactions and some consistency checking?
func (ds *DatastoreSaver) SaveTask(ctx context.Context, t Task) error {
	k := datastore.NameKey("task", t.Name, nil)
	k.Namespace = ds.Namespace
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := ds.Client.Put(ctx, k, &t)
	if err != nil {
		return err
	}
	return ctx.Err()
}

// DeleteTask implements Saver.DeleteTask using Datastore.
func (ds *DatastoreSaver) DeleteTask(ctx context.Context, t Task) error {
	k := datastore.NameKey("task", t.Name, nil)
	k.Namespace = ds.Namespace
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := ds.Client.Delete(ctx, k)
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Task contains the state of a single Task.
// These will be stored and retrieved from DataStore. The fact that this struct
// is written to datastore means that it can not be an interface and instead
// must be a struct.
type Task struct {
	Name       string // e.g. gs://archive-mlab-oti/ndt/2017/06/01/
	Experiment string // e.g. ndt, sidestream, etc.
	Date       time.Time
	State      State
	Queue      string // The queue the task files are submitted to, or "".
	JobID      string // BigQuery JobID, when the state is Deduplicating
	ErrMsg     string // Task handling error, if any
	ErrInfo    string // More context about any error, if any

	UpdateTime time.Time

	saver Saver // Saver is used for Save operations. Stored locally, but not persisted.
}

// GetExperiment parses the input string like "gs://archive-mlab-oti/ndt/2017/06/01/"
// and return "ndt"
func GetExperiment(name string) (string, error) {
	split := strings.Split(name, "/")
	if len(split) < 5 {
		return "", errors.New("Incorrect input name")
	}
	return split[3], nil
}

// NewTask properly initializes a new task, complete with saver.
func NewTask(name string, queue string, saver Saver) (*Task, error) {
	expt, err := GetExperiment(name)
	if err != nil {
		return nil, err
	}
	t := Task{Name: name, Experiment: expt, State: Initializing, Queue: queue, saver: saver}
	t.UpdateTime = time.Now()
	parts, err := t.ParsePrefix()
	if err != nil {
		return nil, err
	}
	date, err := time.Parse("2006/01/02", parts[2])
	if err != nil {
		return nil, err
	}
	t.Date = date
	return &t, nil
}

const start = `^gs://(?P<bucket>.*)/(?P<exp>[^/]*)/`
const datePath = `(?P<datepath>\d{4}/[01]\d/[0123]\d)/`

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	prefixPattern = regexp.MustCompile(start + // #1 #2
		datePath) // #3 - YYYY/MM/DD
)

// ParsePrefix Parses prefix, returning {bucket, experiment, date string}, error
// Unless it returns error, the result will be exactly length 3.
func (t *Task) ParsePrefix() ([]string, error) {
	fields := prefixPattern.FindStringSubmatch(t.Name)

	if fields == nil {
		return nil, errors.New("Invalid test path: " + t.Name)
	}
	// If fields is not nil, then there was a match, and all matches contain 4 fields.
	return fields[1:], nil
}

// SourceAndDest creates BQ Table entities for the source templated table, and destination partition.
func (t *Task) SourceAndDest(ds *dataset.Dataset) (bqiface.Table, bqiface.Table, error) {
	// Launch the dedup request, and save the JobID
	parts, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		metrics.FailCount.WithLabelValues("BadDedupPrefix")
		return nil, nil, err
	}

	tableName := etl.DirToTablename(parts[1])

	src := ds.Table(tableName + "_" + strings.Join(strings.Split(parts[2], "/"), ""))
	dest := ds.Table(tableName + "$" + strings.Join(strings.Split(parts[2], "/"), ""))
	return src, dest, nil
}

func (t Task) String() string {
	return fmt.Sprintf("{%s: %s, %s, Q:%s, J:%s, E:%s (%s)}", t.Name, StateNames[t.State],
		t.UpdateTime.Format("Mon15:04:05.0"), t.Queue, t.JobID, t.ErrMsg, t.ErrInfo)
}

// ErrNoSaver is returned when saver has not been set.
var ErrNoSaver = errors.New("Task.saver is nil")

// Save saves the task state to the "saver".
func (t *Task) Save(ctx context.Context) error {
	if t.saver == nil {
		return ErrNoSaver
	}
	t.UpdateTime = time.Now()
	metrics.StateDate.WithLabelValues(StateNames[t.State]).Set(float64(t.Date.Unix()))
	return t.saver.SaveTask(ctx, *t)
}

// Update updates the task state, and saves to the "saver".
func (t *Task) Update(ctx context.Context, st State) error {
	duration := t.UpdateTime.Sub(time.Now())
	metrics.StateTimeSummary.WithLabelValues(StateNames[t.State]).Observe(duration.Seconds())
	t.State = st
	t.UpdateTime = time.Now()
	metrics.StateDate.WithLabelValues(StateNames[t.State]).Set(float64(t.Date.Unix()))
	if t.saver == nil {
		return ErrNoSaver
	}
	return t.saver.SaveTask(ctx, *t)
}

// Delete removes by calling saver.DeleteTask.
func (t *Task) Delete(ctx context.Context) error {
	if t.saver == nil {
		return ErrNoSaver
	}
	return t.saver.DeleteTask(ctx, *t)
}

// SetError adds error information and saves to the "saver"
func (t *Task) SetError(ctx context.Context, err error, info string) error {
	metrics.FailCount.WithLabelValues(info)
	if t.saver == nil {
		return ErrNoSaver
	}
	t.ErrMsg = err.Error()
	t.ErrInfo = info
	t.UpdateTime = time.Now()
	log.Println("SetError:", t)
	return t.saver.SaveTask(ctx, *t)
}

// SetSaver sets the value of the saver to be used for all other calls.
func (t *Task) SetSaver(saver Saver) {
	t.saver = saver
}

// Terminator interface provides notification and synchronization for termination.
type Terminator interface {
	GetNotifyChannel() <-chan struct{}
	Terminate()
	// This is also the sync.WaitGroup API
	Add(n int)
	Done()
	Wait()
}

// Process handles all steps of processing a task.
func (t Task) Process(ctx context.Context, ex Executor, doneWithQueue func(), term Terminator) {
	metrics.TasksInFlight.Inc()
	defer metrics.TasksInFlight.Dec()
loop:
	for t.State != Done { //&& t.ErrMsg == "" {
		select {
		case <-term.GetNotifyChannel():
			t.SetError(ctx, ErrTaskSuspended, "Terminating")
			break loop
		default:
			q := t.Queue
			if err := ex.Next(ctx, &t, term.GetNotifyChannel()); err != nil {
				break loop
			}
			if q != "" && t.Queue == "" {
				// We transitioned to a state that no longer requires the queue.
				log.Printf("returning queue from %s %p\n", t.Name, &t)
				doneWithQueue()
			}
		}
	}
	if t.ErrMsg == "" {
		// Only delete the state entry if it completed without error.
		t.Delete(ctx)
		metrics.CompletedCount.WithLabelValues("todo - add exp type").Inc()
	} else {
		metrics.CompletedCount.WithLabelValues(StateNames[t.State] + " Error").Inc()
	}
	term.Done()
}

// GetStatus fetches all Task state of request experiment from Datastore.
// If expt is empty string, return all tasks.
func (ds *DatastoreSaver) GetStatus(ctx context.Context, expt string) ([]Task, error) {
	q := datastore.NewQuery("task").Namespace(ds.Namespace).Filter("Experiment =", expt)
	tasks := make([]Task, 0, 100)
	_, err := ds.Client.GetAll(ctx, q, &tasks)
	if err != nil {
		return nil, err
		// Handle error.
	}
	return tasks, nil
}

// WriteHTMLStatusTo writes HTML formatted task status.
func WriteHTMLStatusTo(ctx context.Context, w io.Writer, project string, expt string) error {
	ds, err := NewDatastoreSaver(ctx, project)
	defer ds.Client.Close()
	if err != nil {
		fmt.Fprintln(w, "Error creating Datastore client:", err)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tasks, err := ds.GetStatus(ctx, expt)
	if err != nil {
		fmt.Fprintln(w, "Error executing Datastore query:", err)
		fmt.Fprintln(w, "Project:", project)
		log.Println(project, err)
		return err
	}

	if ctx.Err() != nil {
		fmt.Fprintln(w, "Context error executing Datastore query:", err)
		return err
	}
	fmt.Fprintf(w, "<div>\nTask State</br>\n")
	for i := range tasks {
		fmt.Fprintf(w, "%s</br>\n", tasks[i])
	}
	fmt.Fprintf(w, "</div>\n")
	return nil
}
