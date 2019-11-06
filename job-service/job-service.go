// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import "time"

// A Job describes a job dispatched to a Parser.
type Job struct {
	FullPrefix string // The GCS bucket/prefix defining the Job scope.

	LastTaskCompleted string // The complete path to the last task file completed.
	State             string // String defining the current state - working, completed, failed.
	Errors            []string
}

// Service contains all information needed to provide a job service.  It iterates through
// jobs using a
type Service struct {
	config Config

	CurrentDate time.Time // The date currently being dispatched.
	PrefixIndex int       // index of Config.TypePrefixes to dispatch next.
}

// NextJob returns a Job object to dispatch.
func (svc *Service) NextJob() *Job {

	return nil
}
