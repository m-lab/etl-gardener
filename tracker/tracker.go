// Package tracker tracks status of all jobs, and handles persistence.
//
// Concurrency properties:
//  1. The job map is protected by a Mutex, but lock is only required
//     to get a copy or set the JobState value, so there is minimal
//     contention.
//  2. JobState objects are persisted to a Saver by a separate
//     goroutine that periodically updates any modified JobState objects.
//     The JobState's updatetime is used to determine whether it needs
//     to be saved.
package tracker

import (
	"context"
	"errors"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/persistence"
	"golang.org/x/sync/errgroup"
)

// Error declarations
var (
	ErrJobAlreadyExists       = errors.New("job already exists")
	ErrJobNotFound            = errors.New("job not found")
	ErrJobIsObsolete          = errors.New("job is obsolete")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrNotYetImplemented      = errors.New("not yet implemented")
)

// State types are used for the JobState.State values
// This is intended to enforce type safety, but compiler accepts string assignment.  8-(
type State string

// State values
const (
	Init          State = "init"
	Parsing       State = "parsing"
	Stabilizing   State = "stabilizing"
	Deduplicating State = "deduplicating"
	Joining       State = "joining"
	Failed        State = "failed"
	Complete      State = "complete"
)

// A JobState describes the state of a bucket/exp/type/YYYY/MM/DD job.
// Completed jobs are removed from the persistent store.
// Errored jobs are maintained in the persistent store for debugging.
// JobState should be updated only by the Tracker, which will
// ensure correct serialization and Saver updates.
type JobState struct {
	persistence.Base // Includes Name field like gs://bucket/exp/type/YYYY/MM/DD

	UpdateTime    time.Time // Time of last update.
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	State     State  // String defining the current state.
	LastError string // The most recent error encountered.

	// Not persisted
	errors []string // all errors related to the job.
}

func assertStateObject(so persistence.StateObject) {
	assertStateObject(JobState{})
}

// GetKind implements Saver.GetKind
func (j JobState) GetKind() string {
	return reflect.TypeOf(j).Name()
}

// saveOrDelete saves the JobState to the persistence.Saver, or, if
// job.State is "Complete", it removes it using Saver.Delete.
// This is threadsafe, but concurrent calls on the same job create a
// race in the Saver, so the final value is unclear.
func (j JobState) saveOrDelete(s persistence.Saver) error {
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	if j.isDone() {
		return s.Delete(ctx, &j)
	}
	return s.Save(ctx, &j)
}

func (j JobState) isDone() bool {
	return j.State == Complete
}

// NewJobState creates a new JobState with provided name.
func NewJobState(name string) JobState {
	return JobState{
		Base:   persistence.NewBase(name),
		errors: make([]string, 0, 1),
		State:  "Init",
	}
}

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	saver          persistence.Saver
	ticker         *time.Ticker
	saveLock       sync.Mutex // Mutex held during save cycles.
	lastUpdateTime time.Time  // Last time updates were saved.

	lock sync.Mutex

	jobs map[string]*JobState // Map from prefix to JobState.
}

// InitTracker recovers the Tracker state from a Saver object.
// May return error if recovery fails.
func InitTracker(saver persistence.Saver, saveInterval time.Duration) (*Tracker, error) {
	// TODO implement recovery.
	t := Tracker{saver: saver, jobs: make(map[string]*JobState, 100)}
	if saveInterval > 0 {
		t.saveEvery(saveInterval)
	}
	return &t, nil
}

// NumJobs returns the number of jobs in flight.  This includes
// jobs in "Complete" state that have not been removed from saver.
func (tr *Tracker) NumJobs() int {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return len(tr.jobs)
}

// Sync synchronously saves all jobs to saver, removing completed jobs
// from tracker.
func (tr *Tracker) Sync() {
	tr.saveAllModifiedJobs()
}

// SaveAllModifiedJobs snapshots all modified jobs and concurrently saves
// them.  When saving is complete, func returns nil, or the first error,
// if there were any errors.
func (tr *Tracker) saveAllModifiedJobs() error {
	tr.saveLock.Lock() // Hold the lock while saving.
	defer tr.saveLock.Unlock()

	last := tr.lastUpdateTime
	tr.lastUpdateTime = time.Now()
	eg := errgroup.Group{}
	tr.lock.Lock()
	// Start concurrent save of all jobs.
	for key, job := range tr.jobs { // TODO - is job capture correct here?
		if job.UpdateTime.After(last) || job.HeartbeatTime.After(last) {
			jobCopy := *job
			f := func() error {
				err := jobCopy.saveOrDelete(tr.saver)
				// We will eventually return only one error, so log all errors.
				if err != nil {
					log.Println(err)
				}
				return err
			}
			eg.Go(f)
			if job.isDone() {
				delete(tr.jobs, key)
			}
		}
	}
	// We have copied all jobs, so we can release the lock.
	tr.lock.Unlock()
	return eg.Wait()
}

func (tr *Tracker) saveEvery(interval time.Duration) {
	tr.ticker = time.NewTicker(interval)
	go func() {
		for range tr.ticker.C {
			tr.saveAllModifiedJobs()
		}
	}()
}

// GetJob gets a copy of an existing job.
func (tr *Tracker) getJob(prefix string) (JobState, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	job := tr.jobs[prefix]
	if job == nil {
		return JobState{}, ErrJobNotFound
	}
	return *job, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists.
func (tr *Tracker) AddJob(prefix string) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[prefix]
	if ok {
		return ErrJobAlreadyExists
	}
	job := NewJobState(prefix)

	tr.jobs[prefix] = &job
	return nil
}

// updateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) updateJob(job JobState) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	oldJob, ok := tr.jobs[job.Name]
	if !ok || oldJob.isDone() {
		return ErrJobNotFound
	}

	tr.jobs[job.Name] = &job
	return nil
}

// SetJobState updates a job's state, and handles persistence.
func (tr *Tracker) SetJobState(prefix string, newState State) error {
	job, err := tr.getJob(prefix)
	if err != nil {
		return err
	}
	job.State = newState
	job.UpdateTime = time.Now()
	return tr.updateJob(job)
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(prefix string) error {
	job, err := tr.getJob(prefix)
	if err != nil {
		return err
	}
	job.HeartbeatTime = time.Now()
	return tr.updateJob(job)
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(prefix string, errString string) error {
	job, err := tr.getJob(prefix)
	if err != nil {
		return err
	}
	job.UpdateTime = time.Now()
	job.LastError = errString
	job.errors = append(job.errors, errString)
	return tr.updateJob(job)
}
