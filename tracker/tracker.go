// Package tracker tracks status of all jobs, and handles persistence.
//
// Concurrency properties:
//  1. The job map is protected by a Mutex, but lock is only required
//     to get a copy or set the Status value, so there is minimal
//     contention.
//  2. Status objects are persisted to a Saver by a separate
//     goroutine that periodically updates any modified Status objects.
//     The Status's updatetime is used to determine whether it needs
//     to be saved.
package tracker

import (
	"context"
	"encoding/json"
	"errors"
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
// NB:  The date will be converted to UTC and truncated to day boundary!
func NewJob(bucket, exp, typ string, date time.Time) Job {
	return Job{Bucket: bucket,
		Experiment: exp,
		Datatype:   typ,
		Date:       date.UTC().Truncate(24 * time.Hour)}
}

// Error declarations
var (
	ErrClientIsNil            = errors.New("nil datastore client")
	ErrJobAlreadyExists       = errors.New("job already exists")
	ErrJobNotFound            = errors.New("job not found")
	ErrJobIsObsolete          = errors.New("job is obsolete")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrNotYetImplemented      = errors.New("not yet implemented")
)

// State types are used for the Status.State values
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
	// Note that GetStatus will never return Complete, as the
	// Job is removed when SetJobState is called with Complete.
	Complete State = "complete"
)

// A Status describes the state of a bucket/exp/type/YYYY/MM/DD job.
// Completed jobs are removed from the persistent store.
// Errored jobs are maintained in the persistent store for debugging.
// Status should be updated only by the Tracker, which will
// ensure correct serialization and Saver updates.
type Status struct {
	UpdateTime    time.Time // Time of last update.
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	State     State  // String defining the current state.
	LastError string // The most recent error encountered.

	// Note that these are not persisted
	errors []string // all errors related to the job.
}

func (j Status) isDone() bool {
	return j.State == Complete
}

// NewStatus creates a new Status with provided parameters.
func NewStatus() Status {
	return Status{
		State:  Init,
		errors: make([]string, 0, 1),
	}
}

// JobMap is defined to allow custom json marshal/unmarshal.
// It defines the map from Job to Status.
// TODO implement datastore.PropertyLoadSaver
type JobMap map[Job]Status

// MarshalJSON implements json.Marshal
func (jobs JobMap) MarshalJSON() ([]byte, error) {
	type Pair struct {
		Job   Job
		State Status
	}
	pairs := make([]Pair, len(jobs))
	i := 0
	for k, v := range jobs {
		pairs[i].Job = k
		pairs[i].State = v
		i++
	}
	return json.Marshal(&pairs)
}

// UnmarshalJSON implements json.UnmarshalJSON
// jobs and data should be non-nil.
func (jobs *JobMap) UnmarshalJSON(data []byte) error {
	type Pair struct {
		Job   Job
		State Status
	}
	pairs := make([]Pair, 0, 100)
	err := json.Unmarshal(data, &pairs)
	if err != nil {
		return err
	}

	for i := range pairs {
		(*jobs)[pairs[i].Job] = pairs[i].State
	}
	return nil
}

// saverStruct is used only for saving and loading from datastore.
type saverStruct struct {
	SaveTime time.Time
	Jobs     []byte `datastore:",noindex"`
}

// loadJobMap loads the persisted map of jobs in flight.
func loadJobMap(ctx context.Context, client dsiface.Client, key *datastore.Key) (JobMap, error) {
	if client == nil {
		return nil, ErrClientIsNil
	}
	state := saverStruct{time.Time{}, make([]byte, 0, 100000)}

	err := client.Get(ctx, key, &state) // This should error?
	if err != nil {
		return nil, err
	}
	jobMap := make(JobMap, 100)
	log.Println("Unmarshalling", len(state.Jobs))
	err = json.Unmarshal(state.Jobs, &jobMap)
	if err != nil {
		return nil, err
	}
	return jobMap, nil
}

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key
	ticker *time.Ticker

	// The lock should be held whenever accessing the jobs JobMap
	lock sync.Mutex
	jobs JobMap // Map from Job to Status.
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(ctx context.Context, client dsiface.Client, key *datastore.Key, saveInterval time.Duration) (*Tracker, error) {
	// TODO implement recovery.
	jobMap, err := loadJobMap(ctx, client, key)
	if err != nil {
		log.Println(err, key)
		jobMap = make(JobMap, 100)
	}
	t := Tracker{client: client, dsKey: key, jobs: jobMap}
	if client != nil && saveInterval > 0 {
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

// getJSON creates a json encoding of the job map.
func (tr *Tracker) getJSON() ([]byte, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
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

// GetStatus retrieves the status of an existing job.
func (tr *Tracker) GetStatus(job Job) (Status, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return Status{}, ErrJobNotFound
	}
	return status, nil
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

	state := NewStatus()
	tr.jobs[job] = state
	return nil
}

// updateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) updateJob(job Job, state Status) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}

	if state.isDone() {
		delete(tr.jobs, job)
	} else {
		tr.jobs[job] = state
	}
	return nil
}

// SetStatus updates a job's state, and handles persistence.
func (tr *Tracker) SetStatus(job Job, newState State) error {
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

// GetAll returns the full job map.
func (tr *Tracker) GetAll() JobMap {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	m := make(JobMap, len(tr.jobs))
	for k, v := range tr.jobs {
		m[k] = v
	}
	return m
}
