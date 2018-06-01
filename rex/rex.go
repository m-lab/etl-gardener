// Package rex ...
package rex

// TODO - work out how to manage queues and new tasks.
// Some simplificaitons:
// 1. available queues should come from config, not state.
// 2. Tasks contain all info about queues in use.
// 3. Don't need to list the tasks in the persistent state, as they are available from DS

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/dispatch"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

// SaveSystem saves the top level system state.
func (s *DatastoreSaver) SaveSystem(ss *ReprocState) error { return nil }

// DatastoreSaver will implement a Saver that stores Task state in Datastore.
type DatastoreSaver struct {
	// TODO - add datastore stuff
}

// SaveTask implements Saver.SaveTask using Datastore.
func (s *DatastoreSaver) SaveTask(t state.Task) error {
	log.Println("Saving", t)
	return nil
}

// DeleteTask implements Saver.DeleteTask using Datastore.
func (s *DatastoreSaver) DeleteTask(t state.Task) error {
	log.Println("Deleting", t)
	return nil
}

// Loader provides API for persisting and retrieving state.
type Loader interface {
	LoadFromPS(ss *ReprocState)
}

// SysSaver provides API for persisting system state.
type SysSaver interface {
	SaveSystem(s *ReprocState) error
}

// ReprocessingExecutor handles all reprocessing steps.
type ReprocessingExecutor struct {
	Project    string
	Dataset    string
	Options    []option.ClientOption
	Client     *http.Client
	BucketOpts []option.ClientOption
}

// AdvanceState advances task to the next state.
func (rex *ReprocessingExecutor) AdvanceState(t *state.Task) {
	switch t.State {
	case state.Initializing:
		t.State = state.Queuing
	case state.Queuing:
		t.State = state.Processing
	case state.Processing:
		t.Queue = "" // No longer need to keep the queue.
		t.State = state.Stabilizing
	case state.Stabilizing:
		t.State = state.Deduplicating
	case state.Deduplicating:
		t.State = state.Finishing
	case state.Finishing:
		t.State = state.Done
	}
}

// GetDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetDS() (bqext.Dataset, error) {
	return bqext.NewDataset(rex.Project, rex.Dataset, rex.Options...)
}

// ErrNoSaver is returned when saver has not been set.
var ErrNoSaver = errors.New("Task.saver is nil")

// DoAction executes the currently specified action and update Err/ErrInfo
// It may terminate prematurely if terminate is closed prior to completion.
// TODO - consider returning any error to caller.
func (rex *ReprocessingExecutor) DoAction(t *state.Task, terminate <-chan struct{}) {
	log.Println("Do", t)
	// Note that only the state in state.Task is maintained between actions.
	switch t.State {
	case state.Initializing:
		// Nothing to do.
	case state.Queuing:
		rex.queue(t, terminate)
	case state.Processing: // TODO should this be Parsing?
		rex.waitForParsing(t, terminate)
	case state.Stabilizing:
		// Wait for the streaming buffer to be nil.
		ds, err := rex.GetDS()
		if err != nil {
			t.SetError(err, "GetDS")
			return
		}
		s, _, err := t.SourceAndDest(&ds)
		if err != nil {
			t.SetError(err, "SourceAndDest")
			return
		}
		err = dispatch.WaitForStableTable(s)
		if err != nil {
			t.SetError(err, "WaitForStableTable")
			return
		}
	case state.Deduplicating:
		rex.dedup(t, terminate)
	case state.Finishing:
		rex.finish(t, terminate)
	}
}

// TODO should these take Task instead of *Task?
func (rex *ReprocessingExecutor) waitForParsing(t *state.Task, terminate <-chan struct{}) {
	// Wait for the queue to drain.
	// Don't want to accept a date until we can actually queue it.
	qh, err := tq.NewQueueHandler(rex.Client, rex.Project, t.Queue)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewQueueHandler")
		t.SetError(err, "NewQueueHandler")
		return
	}
	log.Println("Wait for empty queue ", qh.Queue)
	for err := qh.IsEmpty(); err != nil; err = qh.IsEmpty() {
		select {
		case <-terminate:
			t.SetError(err, "Terminating")
			return
		default:
		}
		if err == tq.ErrMoreTasks {
			// Wait 5-15 seconds before checking again.
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		} else if err != nil {
			// We don't expect errors here, so try logging, and a large backoff
			// in case there is some bad network condition, service failure,
			// or perhaps the queue_pusher is down.
			log.Println(err)
			metrics.WarningCount.WithLabelValues("IsEmptyError").Inc()
			// TODO update metric
			time.Sleep(time.Duration(60+rand.Intn(120)) * time.Second)
		}
	}
}

func (rex *ReprocessingExecutor) queue(t *state.Task, terminate <-chan struct{}) {
	// Submit all files from the bucket that match the prefix.
	// Where do we get the bucket?
	//func (qh *ChannelQueueHandler) handleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	qh, err := tq.NewQueueHandler(rex.Client, rex.Project, t.Queue)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewQueueHandler")
		t.SetError(err, "NewQueueHandler")
		return
	}
	parts, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		metrics.FailCount.WithLabelValues("BadPrefix").Inc()
		t.SetError(err, "BadPrefix")
		return
	}
	bucketName := parts[1]
	bucket, err := tq.GetBucket(rex.BucketOpts, rex.Project, bucketName, false)
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("BucketError").Inc()
		t.SetError(err, "BucketError")
		return
	}
	// TODO maybe check terminate while queuing?
	n, err := qh.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("PostDayError").Inc()
		t.SetError(err, "PostDayError")
		return
	}
	log.Println("Added ", n, t.Name, " tasks to ", qh.Queue)
}

func (rex *ReprocessingExecutor) dedup(t *state.Task, terminate <-chan struct{}) {
	// Launch the dedup request, and save the JobID
	ds, err := rex.GetDS()
	if err != nil {
		metrics.FailCount.WithLabelValues("NewDataset")
		t.SetError(err, "GetDS")
		return
	}
	src, dest, err := t.SourceAndDest(&ds)
	if err != nil {
		metrics.FailCount.WithLabelValues("SourceAndDest")
		t.SetError(err, "SourceAndDest")
		return
	}

	log.Println("Dedupping", src.FullyQualifiedName())
	// TODO move Dedup??
	job, err := dispatch.Dedup(&ds, src.TableID, dest)
	if err != nil {
		if err == io.EOF {
			log.Println("EOF error - is this a test client?")
		} else {
			log.Println(err, src.FullyQualifiedName())
			metrics.FailCount.WithLabelValues("DedupFailed")
			t.SetError(err, "DedupFailed")
			return
		}
	}
	t.JobID = job.ID()
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - move this to go/bqext, since it is bigquery specific and general purpose.
func waitForJob(ctx context.Context, job *bigquery.Job, maxBackoff time.Duration, terminate <-chan struct{}) error {
	backoff := 10 * time.Millisecond
	previous := backoff
	for {
		select {
		case <-terminate:
			return state.ErrTaskSuspended
		default:
		}
		status, err := job.Status(ctx)
		if err != nil {
			continue
		}
		if status.Err() != nil {
			continue
		}
		if status.Done() {
			break
		}
		if backoff+previous < maxBackoff {
			tmp := previous
			previous = backoff
			backoff = backoff + tmp
		} else {
			backoff = maxBackoff
		}

		time.Sleep(backoff)
	}
	return nil
}

func (rex *ReprocessingExecutor) finish(t *state.Task, terminate <-chan struct{}) {
	// TODO use a simple client instead of creating dataset?
	ds, err := bqext.NewDataset(rex.Project, rex.Dataset, rex.Options...)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewDataset")
		t.SetError(err, "NewDataset")
		return
	}
	src, _, err := t.SourceAndDest(&ds)
	if err != nil {
		metrics.FailCount.WithLabelValues("SourceAndDest")
		t.SetError(err, "SourceAndDest")
		return
	}
	job, err := ds.BqClient.JobFromID(context.Background(), t.JobID)
	if err != nil {
		metrics.FailCount.WithLabelValues("JobFromID")
		t.SetError(err, "JobFromID")
		return
	}
	// TODO - should loop, and check terminate channel
	err = waitForJob(context.Background(), job, 10*time.Second, terminate)
	status, err := job.Wait(context.Background())
	if err != nil {
		if err != state.ErrTaskSuspended {
			log.Println(status.Err(), src.FullyQualifiedName())
			metrics.FailCount.WithLabelValues("DedupJobWait")
			t.SetError(err, "DedupJobWait")
		}
		return
	}

	// Wait for JobID to complete, then delete the template table.
	log.Println("Completed deduplication, deleting", src.FullyQualifiedName())
	// If deduplication was successful, we should delete the source table.
	ctx, cf := context.WithTimeout(context.Background(), time.Minute)
	defer cf()
	err = src.Delete(ctx)
	if err != nil {
		metrics.FailCount.WithLabelValues("TableDeleteErr")
		log.Println(err)
	}
	if ctx.Err() != nil {
		if ctx.Err() != context.DeadlineExceeded {
			metrics.FailCount.WithLabelValues("TableDeleteTimeout")
			t.SetError(err, "TableDeleteTimeout")
			log.Println(ctx.Err())
			return
		}
	}

	// TODO - Copy to base_tables.
}

// ReprocState holds the high level state of the reprocessing dispatcher.
// Note that the DataStore representation will have separate entries for
// each map entry.
type ReprocState struct {
	NextDate time.Time           // The date about to be queued, or to be queued next.
	AllTasks map[string]struct{} // set of names of all tasks in flight.

	// This is used internally, e.g. for Describe
	// Basically a cache of all the task states.
	allTasks map[string]state.Task // Cache from task names to Task

	lock        *sync.Mutex
	queues      chan string // Names of idle task queues.
	terminating bool        // Indicates when tasks are being terminated.
	terminate   chan<- struct{}
	updateChan  <-chan state.Task
	sysSaver    SysSaver
	helpers     state.Helpers

	wg *sync.WaitGroup // Tracks how many active Tasks there are.
}

// String formats the ReprocState for printing.
func (ss ReprocState) String() string {
	return fmt.Sprintf("{%s %s}", ss.NextDate.Format("2006/01/02"), ss.AllTasks)
}

// Thread safety invariants for ReprocState
//  1. Only MonitorTasks updates the AllTasks/allTasks maps.
//  2. All accesses to NextDate and AllTasks are protected with Mutex "lock"
//  3. Updates to persistent store are lazy for Task removals.  AllTask inserts
//      and NextDate updates are always pushed to Saver before launching task. ????

func newReprocState(ex state.Executor, taskSaver state.Saver, sysSaver SysSaver) *ReprocState {
	lock := sync.Mutex{}
	queues := make(chan string, 100) // Don't want to block on returning a queue.
	taskNames := make(map[string]struct{}, 20)
	taskMap := make(map[string]state.Task, 20)
	updater := make(chan state.Task)
	terminate := make(chan struct{})
	wg := sync.WaitGroup{}

	sysState := ReprocState{NextDate: time.Time{}, AllTasks: taskNames,
		allTasks:    taskMap,
		lock:        &lock,
		queues:      queues,
		terminating: false,
		terminate:   terminate,
		updateChan:  updater,
		helpers:     state.NewHelpers(ex, taskSaver, updater, terminate), sysSaver: sysSaver, wg: &wg}
	sysState.wg.Add(1)
	return &sysState
}

// LoadAndInitReprocState creates a new system state object, and initializes it from ps.
func LoadAndInitReprocState(ldr Loader, ex state.Executor, taskSaver state.Saver, sysSaver SysSaver) *ReprocState {
	ss := newReprocState(ex, taskSaver, sysSaver)
	// Set up the go routine that listens for Task state changes.
	go ss.MonitorTasks()
	// This loads initial state and calls Add() on each
	// existing task.
	ldr.LoadFromPS(ss)

	// Caller should call StartNextTask() on any unused queues ??
	return ss
}

// MonitorTasks processes input from updateChan.
func (ss *ReprocState) MonitorTasks() {
	for t := range ss.updateChan {
		// Update local cache (persistent store should be updated by Task)
		if t.Queue == "" && ss.allTasks[t.Name].Queue != "" {
			ss.queues <- ss.allTasks[t.Name].Queue
		}
		ss.allTasks[t.Name] = t
		if t.State == state.Done {
			ss.lock.Lock()
			delete(ss.allTasks, t.Name)
			delete(ss.AllTasks, t.Name)
			ss.sysSaver.SaveSystem(ss)
			ss.lock.Unlock()
			ss.wg.Done()
		} else if t.IsSuspended() {
			ss.wg.Done()
		}
	}
}

// Describe writes a description of the complete state to w
func (ss *ReprocState) Describe(w *io.Writer) {

}

// Add adds a single task.  Msg back to updateChan will update local state and DS
func (ss *ReprocState) Add(t state.Task) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	if !ss.terminating {
		ss.AllTasks[t.Name] = struct{}{}
		ss.sysSaver.SaveSystem(ss)
		ss.wg.Add(1)
		go t.Process(ss.helpers)
	}
}

// Terminate signals all Tasks to terminate
func (ss *ReprocState) Terminate() {
	log.Println("Terminating")
	ss.terminating = true
	close(ss.terminate)
}

// WaitForTerminate waits until all Tasks have terminated.
func (ss *ReprocState) WaitForTerminate() {
	ss.wg.Done() // Consume wait for ReprocState
	ss.wg.Wait() // Wait until all Tasks have completed.
}

// DoDispatchLoop just sequences through archives in date order.
// It will generally be blocked on the queues.
func (ss *ReprocState) DoDispatchLoop(bucket string, experiments []string, startDate time.Time) {
	for {
		for _, e := range experiments {
			time.Sleep(5 * time.Millisecond) // Good for testing, and negligible for production.
			select {
			case <-ss.helpers.Terminate:
				return
			case q := <-ss.queues:
				prefix := ss.NextDate.Format(fmt.Sprintf("gs://%s/%s/2006/01/02/", bucket, e))
				t := state.Task{Name: prefix, State: state.Initializing, Queue: q}
				// This may block waiting for a queue.ch
				ss.Add(t)
			}
		}

		ss.NextDate = ss.NextDate.AddDate(0, 0, 1)

		// If gardener has processed all dates up to two days ago,
		// start over.
		if ss.NextDate.Add(48 * time.Hour).After(time.Now()) {
			// TODO - load this from DataStore
			ss.NextDate = startDate
		}
	}
}
