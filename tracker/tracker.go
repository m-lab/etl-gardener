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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/datastore/dsiface"
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
	key string // cache of key string, not persisted

	// These define the job.
	Bucket     string
	ExpAndType string
	Date       time.Time

	UpdateTime    time.Time // Time of last update.
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	State     State  // String defining the current state.
	LastError string // The most recent error encountered.

	// Note that these are not persisted
	errors []string // all errors related to the job.
}

// JobKey creates a canonical key for a given bucket, exp, and date.
func JobKey(bucket string, exp string, date time.Time) string {
	return fmt.Sprintf("gs://%s/%s/%s",
		bucket, exp, date.Format("2006/01/02"))
}

// Key implements Saver.Key
func (j JobState) Key() string {
	if len(j.key) == 0 {
		return JobKey(j.Bucket, j.ExpAndType, j.Date)
	}
	return j.key
}

func (j JobState) isDone() bool {
	return j.State == Complete
}

// NewJobState creates a new JobState with provided parameters.
// NB:  The date will be converted to UTC and truncated to day boundary!
func NewJobState(bucket string, exp string, date time.Time) JobState {
	return JobState{
		Bucket:     bucket,
		ExpAndType: exp,
		Date:       date.UTC().Truncate(24 * time.Hour),
		State:      "Init",
		errors:     make([]string, 0, 1),
	}
}

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key
	ticker *time.Ticker

	lock sync.Mutex
	jobs map[string]*JobState // Map from prefix to JobState.
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(ctx context.Context, client dsiface.Client, saveInterval time.Duration) (*Tracker, error) {
	// TODO implement recovery.
	t := Tracker{client: client, jobs: make(map[string]*JobState, 100)}
	t.dsKey = datastore.NameKey("tracker", "state", nil)
	t.dsKey.Namespace = "gardener"

	if client != nil {
		var jsonBytes []byte
		err := client.Get(ctx, t.dsKey, &jsonBytes) // This should error?
		if err != nil {
			if err != datastore.ErrNoSuchEntity {
				return nil, err
			}
			log.Println(err)
		} else {
			log.Println("Unmarshalling", len(jsonBytes))
			json.Unmarshal(jsonBytes, &t.jobs)
		}
	}

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

// Sync snapshots the full job state and saves it to the datastore client.
func (tr *Tracker) Sync() error {
	tr.lock.Lock()

	for key, job := range tr.jobs { // TODO - is job capture correct here?
		if job.isDone() {
			delete(tr.jobs, key)
		}
	}

	bytes, err := json.Marshal(tr.jobs)
	// We have copied all jobs, so we can release the lock.
	tr.lock.Unlock()

	if err != nil {
		return err
	}

	// Save the full state.
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = tr.client.Put(ctx, tr.dsKey, &bytes)

	return err
}

func (tr *Tracker) saveEvery(interval time.Duration) {
	tr.ticker = time.NewTicker(interval)
	go func() {
		for range tr.ticker.C {
			tr.Sync()
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
func (tr *Tracker) AddJob(job JobState) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job.Key()]
	if ok {
		return ErrJobAlreadyExists
	}

	tr.jobs[job.Key()] = &job
	return nil
}

// updateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) updateJob(job JobState) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	oldJob, ok := tr.jobs[job.key]
	if !ok || oldJob.isDone() {
		return ErrJobNotFound
	}

	tr.jobs[job.Key()] = &job
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
