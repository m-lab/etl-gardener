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

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
}

// NewJob creates a new job object.
func NewJob(bucket, exp, typ string, date time.Time) Job {
	return Job{Bucket: bucket,
		Experiment: exp,
		Datatype:   typ,
		Date:       date.UTC().Truncate(24 * time.Hour)}
}

// Key generates the prefix key for lookups.
func (j Job) Key() string {
	return fmt.Sprintf("gs://%s/%s/%s/%s",
		j.Bucket, j.Experiment, j.Datatype, j.Date.Format("2006/01/02"))
}

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
	UpdateTime    time.Time // Time of last update.
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	State     State  // String defining the current state.
	LastError string // The most recent error encountered.

	// Note that these are not persisted
	errors []string // all errors related to the job.
}

func (j JobState) isDone() bool {
	return j.State == Complete
}

// NewJobState creates a new JobState with provided parameters.
// NB:  The date will be converted to UTC and truncated to day boundary!
func NewJobState() JobState {
	return JobState{
		State:  Init,
		errors: make([]string, 0, 1),
	}
}

type JobMap map[Job]*JobState

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key
	ticker *time.Ticker

	lock sync.Mutex
	jobs JobMap // Map from Job to JobState.
}

// MarshalJSON implements json.Marshal
func (jobs JobMap) MarshalJSON() ([]byte, error) {
	type Pair struct {
		Job   Job
		State JobState
	}
	pairs := make([]Pair, len(jobs))
	i := 0
	for k, v := range jobs {
		pairs[i].Job = k
		pairs[i].State = *v
		i++
	}
	return json.Marshal(&pairs)
}

// UnmarshalJSON implements json.UnmarshalJSON
func (jobs *JobMap) UnmarshalJSON(data []byte) error {
	type Pair struct {
		Job   Job
		State JobState
	}
	pairs := make([]Pair, 0, 100)
	err := json.Unmarshal(data, &pairs)
	if err != nil {
		return err
	}

	//jobs = make(map[Job]*JobState, len(pairs))
	for i := range pairs {
		(*jobs)[pairs[i].Job] = &pairs[i].State
	}
	return nil
}

type saverStruct struct {
	SaveTime time.Time
	Jobs     []byte `datastore:",noindex"`
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(ctx context.Context, client dsiface.Client, saveInterval time.Duration) (*Tracker, error) {
	// TODO implement recovery.
	t := Tracker{client: client, jobs: make(map[Job]*JobState, 100)}
	t.dsKey = datastore.NameKey("tracker", "state", nil)
	t.dsKey.Namespace = "gardener"

	if client != nil {
		state := saverStruct{time.Time{}, make([]byte, 0, 100000)}

		err := client.Get(ctx, t.dsKey, &state) // This should error?
		if err != nil {
			if err != datastore.ErrNoSuchEntity {
				return nil, err
			}
			log.Println(err)
		} else {
			log.Println("Unmarshalling", len(state.Jobs))
			json.Unmarshal(state.Jobs, &t.jobs)
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

func (tr *Tracker) getJSON() ([]byte, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	// First delete any completed jobs.
	for key, job := range tr.jobs {
		if job.isDone() {
			delete(tr.jobs, key)
		}
	}
	return json.Marshal(tr.jobs)
}

// Sync snapshots the full job state and saves it to the datastore client.
func (tr *Tracker) Sync() error {
	bytes, err := tr.getJSON()
	if err != nil {
		return err
	}

	// Save the full state.
	state := saverStruct{time.Now(), bytes}
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = tr.client.Put(ctx, tr.dsKey, &state)

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

// GetStatus gets a copy of an existing job.
func (tr *Tracker) GetStatus(job Job) (JobState, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status := tr.jobs[job]
	if status == nil {
		return JobState{}, ErrJobNotFound
	}
	return *status, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists.
func (tr *Tracker) AddJob(job Job) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if ok {
		return ErrJobAlreadyExists
	}

	state := NewJobState()
	tr.jobs[job] = &state
	return nil
}

// updateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) updateJob(job Job, state JobState) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}

	if state.isDone() {
		delete(tr.jobs, job)
	} else {
		tr.jobs[job] = &state
	}
	return nil
}

// SetJobState updates a job's state, and handles persistence.
func (tr *Tracker) SetJobState(job Job, newState State) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.State = newState
	status.UpdateTime = time.Now()
	return tr.updateJob(job, status)
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(job Job) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.HeartbeatTime = time.Now()
	return tr.updateJob(job, status)
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(job Job, errString string) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.UpdateTime = time.Now()
	status.LastError = errString
	status.errors = append(status.errors, errString)
	return tr.updateJob(job, status)
}
