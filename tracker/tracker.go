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

// A Job is a complete definition of a single parser task that we might want to
// track from beginning to end. We expect that it's an amount of work that will
// take a single parser a few hours.
type Job struct {
	Bucket, Experiment, Datatype string
	Date                         time.Time
}

// URL returns a Google Cloud Storage URL. Within that URL prefix (not a
// directory, because technically GCS doesn't have those, so we'll say "URL
// prefix" but it can be thought of like a directory), every file should be
// considered part of the job the parser must do in order to mark this Job as
// complete.
func (j Job) URL() string {
	return fmt.Sprintf("gs://%s/%s/%s/%s", j.Bucket, j.Experiment, j.Datatype, j.Date.Format("2006/01/02"))
}

// Status describes the state of a single bucket/exp/type/YYYY/MM/DD job.
// Status should be updated only by the Tracker, which will
// ensure correct serialization and Saver updates.
type Status struct {
	UpdateTime    time.Time // Time of last update.
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	State     State // String defining the current state.
	LastError error // The most recent error encountered.

	// Note that these are not persisted
	errors []error // all errors related to the job.
}

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key

	lock sync.Mutex
	jobs map[Job]*Status // Map from Job to Status
}

// New recovers the Tracker state from a Client object or creates a new Tracker.
func New(ctx context.Context, client dsiface.Client, saveInterval time.Duration) (*Tracker, error) {
	// TODO implement recovery.
	t := &Tracker{client: client, jobs: make(map[Job]*Status, 100)}
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
		go t.saveEvery(ctx, saveInterval)
	}
	return t, nil
}

// Sync snapshots the full job state and saves it to the datastore client.
func (tr *Tracker) Sync(ctx context.Context) error {
	tr.lock.Lock()

	bytes, err := json.Marshal(tr.jobs)
	// We have copied all jobs, so we can release the lock.
	tr.lock.Unlock()

	if err != nil {
		return err
	}

	// Save the full state.
	ctx, cf := context.WithTimeout(ctx, 10*time.Second)
	defer cf()
	_, err = tr.client.Put(ctx, tr.dsKey, &bytes)

	return err
}

func (tr *Tracker) saveEvery(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			tr.Sync(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// Update updates a job to a new state.
func (tr *Tracker) Update(job Job, state State) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}
	status.State = state
	if state == Complete {
		status.LastError = nil
		status.errors = nil
	}
	status.UpdateTime = time.Now()
	tr.jobs[job] = status
	return nil
}

// Error updates a job with an error to indicate that the job failed.
func (tr *Tracker) Error(job Job, err error) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}
	status.State = Failed
	status.LastError = err
	status.errors = append(status.errors, err)
	status.UpdateTime = time.Now()
	tr.jobs[job] = status
	return nil
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(job Job) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}
	status.HeartbeatTime = time.Now()
	tr.jobs[job] = status
	return nil
}

// NextJob returns the next job that a parser should run.
//
// The job to hand out is the job with:
// 1. A HeartbeatTime in the past by at least the desired threshold.
// 2. The oldest CompletionTime
//
// In the unlikely event that no job can be found with a sufficiently-old
// completion time, return a nil pointer.
func (tr *Tracker) NextJob() (job *Job) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	maxAge := time.Now().Add(-2 * time.Hour)
	var completionTime time.Time
	for j, s := range tr.jobs {
		if s.HeartbeatTime.Before(maxAge) {
			if s.UpdateTime.Before(completionTime) {
				job = &j
				completionTime = s.UpdateTime
			}
		}
	}
	if job != nil {
		tr.jobs[*job].HeartbeatTime = time.Now()
	}
	return job
}

func (tr *Tracker) Handler() Handler {
	return Handler{tr}
}
