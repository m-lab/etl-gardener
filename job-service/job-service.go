// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import "time"

// A Job describes a job dispatched to a Parser.
// This should be moved to JobTracker package.
type Job struct {
	FullPrefix string // The GCS bucket/prefix defining the Job scope.

	ArchivesCompletedThrough string // The complete path to the last task file completed.
	State                    string // String defining the current state - working, completed, failed.
	Errors                   []string
}

// Config provides a config for a job service.
type Config struct {
	Bucket string
}

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	config Config // Currently just a bucket

	CurrentDate   time.Time    // The date currently being dispatched.
	TypeSources   []TypeSource // The sources to be iterated through.
	NextTypeIndex int          // index of TypeSource to dispatch next.
}

// NextJob returns a prefix to dispatch.
func (svc *Service) NextJob() string {
	return ""
}
