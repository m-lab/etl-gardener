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

// ScoreBoard keeps track of all the jobs in flight.
type ScoreBoard struct {
	Jobs map[string]*Job // Map from prefix to Job object.

}

// TypeSource provides a source for prefixes for a particular data type.
type TypeSource struct {
	Bucket     string
	ExpAndType string // like ndt.tcpinfo or ndt or ndt.ndt5
	DateFilter string // optional RegExp to filter dates
	FileFilter string // optional RegExp to filter filenames

	NextJob string // name of next job to dispatch
}

func (ts *TypeSource) Advance() string {
	src := ts.GetNext()

	//... Advance the NextJob value.
	return src
}

func (ts *TypeSource) GetNext() string {
	return ts.NextJob
}

// Config provides a config for a job service.
type Config struct {
	Bucket       string
	TypePrefixes []string // Prefixes of all dates to be handled.
}

// Service contains all information needed to provide a job service.  It iterates through
// jobs using a
type Service struct {
	config Config
	sb     *ScoreBoard

	CurrentDate time.Time // The date currently being dispatched.
	PrefixIndex int       // index of Config.TypePrefixes to dispatch next.
}

// NextJob returns a Job object to dispatch.
func (svc *Service) NextJob() *Job {

	return nil
}
