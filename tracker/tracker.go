// Package tracker tracks status of all jobs, and handles persistence.
//
package tracker

import (
	"fmt"
	"time"
)

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
}

// JobKey creates a canonical key for a given bucket, exp, and date.
func JobKey(bucket string, exp string, typ string, date time.Time) string {
	return fmt.Sprintf("gs://%s/%s/%s/%s",
		bucket, exp, typ, date.Format("2006/01/02"))
}

// Key generates the prefix key for lookups.
func (j Job) Key() string {
	return JobKey(j.Bucket, j.Experiment, j.Datatype, j.Date)
}
