// Package tracker tracks status of all jobs, and handles persistence.
package tracker

import "github.com/m-lab/etl-gardener/persistence"

// A JobState describes the state of a prefix job.
// Completed jobs are removed from the persistent store.
// Errored jobs are maintained in the persistent store for debugging.
// JobState should generally be updated by the Tracker, which will
// ensure correct serialization and Saver updates.
type JobState struct {
	FullPrefix string // The GCS bucket/prefix defining the Job scope.

	State string // String defining the current state - parsing, parsed, dedup, join, completed, failed.

	ArchivesCompletedUpTo string // The path to the last of the contiguous completed archives.

	LastError string // The most recent error encountered.

	// Not persisted
	errors []string // all errors related to the job.
}

// Tracker keeps track of all the jobs in flight.
type Tracker struct {
	saver persistence.Saver

	Jobs map[string]*JobState // Map from prefix to JobState.
}

// InitTracker recovers the Tracker state from a Saver object.
func InitTracker(saver persistence.Saver) (Tracker, error) {
	// TODO implement recovery.

	return Tracker{saver: saver}, nil
}

func (tr *Tracker) SetJobState(prefix string, newState string) error {
	return nil
}

func (tr *Tracker) Job(prefix string, newState string) error {

}
