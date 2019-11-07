// Package tracker tracks status of all jobs, and handles persistence.
//
// Alternative idea for serializing updates to Saver...
//  1. provide a buffered channel to a saver routine for each Job
//  2. send copies of the job to the channel.
//  3. once the channel has the update, further updates are fine.

package tracker

import (
	"context"
	"errors"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/persistence"
)

// Error declarations
var (
	ErrJobAlreadyExists       = errors.New("prefix already exists")
	ErrPrefixNotFound         = errors.New("prefix not found")
	ErrJobIsObsolete          = errors.New("job is obsolete")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrNotYetImplemented      = errors.New("not yet implemented")
)

// A JobState describes the state of a prefix job.
// Completed jobs are removed from the persistent store.
// Errored jobs are maintained in the persistent store for debugging.
// JobState should generally be updated by the Tracker, which will
// ensure correct serialization and Saver updates.
type JobState struct {
	persistence.Base

	State string // String defining the current state - parsing, parsed, dedup, join, completed, failed.

	ArchivesCompletedUpTo string // The path to the last of the contiguous completed archives.

	LastError string // The most recent error encountered.

	// Not persisted
	errors []string // all errors related to the job.
}

// GetKind implements Saver.GetKind
func (j JobState) GetKind() string {
	return reflect.TypeOf(j).Name()
}

// jobWithLock is the object actually saved in the Tracker.  It adds a lock
// object that must be held when accessing or changing the job, and while
// persisting the job to the Saver.
// Saving is generally slow (100 msec or so), and should be done
// asynchronously, holding the Read lock.  However, updates must be
// done holding the write lock, and Go locks cannot be upgraded, so we
// end up having to hold the write lock when Saving, which means other
// users trying to access the job while it is being saved will block.
// TODO - perhaps use copy on write, and separate reader and writer locks?
type jobWithLock struct {
	lock     sync.RWMutex // lock that should be held for all accesses.
	obsolete bool
	JobState
}

// Save takes the lock, and asynchronously saves the state to the Saver.
// This must be used to ensure that saves are properly serialized.
// Caller must own j.lock.Lock(), and must NOT release it.
func (j *jobWithLock) Save(s persistence.Saver) {
	go func() {
		start := time.Now()
		defer j.lock.Unlock()
		ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
		defer cf()
		err := s.Save(ctx, &j.JobState)
		if err != nil {
			// If error, all we can do is log and record the error,
			// TODO - maybe retry?
			log.Println(err)
			j.JobState.LastError = err.Error()
			j.JobState.errors = append(j.JobState.errors, err.Error())
		}
		latency := time.Since(start)
		if latency > 5*time.Second {
			log.Println("Slow update:", j.Name, latency)
		}
	}()
}

// NewJobState creates a new JobState with provided name.
func NewJobState(name string) JobState {
	return JobState{
		Base:   persistence.NewBase(name),
		errors: make([]string, 0, 1),
		State:  "Starting",
	}
}

// Tracker keeps track of all the jobs in flight.
type Tracker struct {
	saver persistence.Saver
	lock  sync.RWMutex

	Jobs map[string]*jobWithLock // Map from prefix to JobState.
}

// InitTracker recovers the Tracker state from a Saver object.
func InitTracker(saver persistence.Saver) (Tracker, error) {
	// TODO implement recovery.

	return Tracker{saver: saver, Jobs: make(map[string]*jobWithLock, 100)}, nil
}

// getJobForUpdate gets a pointer to a jobWithLock entry, with the
// job lock held.
func (tr *Tracker) getJobForUpdate(prefix string) (*jobWithLock, error) {
	start := time.Now()
	tr.lock.Lock()
	job, ok := tr.Jobs[prefix]
	tr.lock.Unlock()
	latency := time.Since(start)
	if latency > time.Second {
		log.Println("Long latency for getJob:", prefix)
	}
	if !ok {
		return nil, ErrPrefixNotFound
	}

	job.lock.Lock() // Take the lock on behalf of job.Save.
	if job.obsolete {
		return nil, ErrJobIsObsolete
	}

	return job, nil
}

// getJobCopy returns a copy of the JobState, with minimal contention.
func (tr *Tracker) getJobCopy(prefix string) (JobState, error) {
	tr.lock.RLock()
	defer tr.lock.RUnlock()
	job, ok := tr.Jobs[prefix]
	if !ok {
		return JobState{}, ErrPrefixNotFound
	}

	return job.JobState, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists.
func (tr *Tracker) AddJob(prefix string) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.Jobs[prefix]
	if ok {
		return ErrJobAlreadyExists
	}
	js := NewJobState(prefix)
	job := jobWithLock{JobState: js}

	job.lock.Lock()    // Take the lock on behalf of job.Save.
	job.Save(tr.saver) // This asynchronously saves the job, and releases the job lock.

	tr.Jobs[prefix] = &job

	return nil
}

// DeleteJob deletes a job from the tracker and persistent store.
func (tr *Tracker) DeleteJob(prefix string) error {
	tr.lock.Lock()
	job, ok := tr.Jobs[prefix]
	if !ok {
		return ErrPrefixNotFound
	}
	delete(tr.Jobs, prefix)
	tr.lock.Unlock()

	// Now no-one else can get it, but another user may already be using it.
	job.lock.Lock() // Take the lock to ensure no-one else is using it.
	defer job.lock.Unlock()
	if job.obsolete {
		// Someone else deleted it, so we are done.
		return ErrJobIsObsolete
	}
	job.obsolete = true // Now no-one else will use it.

	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	err := tr.saver.Delete(ctx, job.JobState)
	return err
}

// SetJobState updates a job's state, and handles persistence.
// We expect ArchivesCompletedUpTo to be updated every minute or so,
// so this function should not allow one job update to block updates
// to other jobs.
func (tr *Tracker) SetJobState(prefix string, newState string) error {
	job, err := tr.getJobForUpdate(prefix)
	if err != nil {
		return err
	}

	job.State = newState
	job.Save(tr.saver) // This asynchronously saves the job, and releases the job lock.
	return nil
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(prefix string, errString string) error {
	job, err := tr.getJobForUpdate(prefix)
	if err != nil {
		return err
	}

	job.LastError = errString
	job.errors = append(job.errors, errString)
	job.Save(tr.saver) // This asynchronously saves the job, and releases the job lock.
	return nil
}
