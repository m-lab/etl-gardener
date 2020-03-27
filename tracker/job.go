package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/bqx"
)

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
}

// JobWithTarget specifies a type/date job, and a destination
// table or GCS prefix
type JobWithTarget struct {
	Job
	// One of these two fields indicates the destination,
	// either a BigQuery table, or a GCS bucket/prefix string.
	TargetTable           bqx.PDT `json:",omitempty"`
	TargetBucketAndPrefix string  `json:",omitempty"` // gs://bucket/prefix
}

// NewJob creates a new job object.
// DEPRECATED
// NB:  The date will be converted to UTC and truncated to day boundary!
func NewJob(bucket, exp, typ string, date time.Time) Job {
	return Job{Bucket: bucket,
		Experiment: exp,
		Datatype:   typ,
		Date:       date.UTC().Truncate(24 * time.Hour),
	}
}

// These are used to limit the number of unique error strings in the FailCount metric.
// After this has been in use a while, we should use a switch statement to categorize
// the error strings, rather than this method.
var errStringLock sync.Mutex
var errStrings = make(map[string]struct{})
var maxUniqueErrStrings = 10

func (j Job) failureMetric(errString string) {
	errStringLock.Lock()
	defer errStringLock.Unlock()
	if _, ok := errStrings[errString]; ok {
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, errString).Inc()
	} else if len(errStrings) < maxUniqueErrStrings {
		errStrings[errString] = struct{}{}
		log.Println("Job failed:", errString)
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, errString).Inc()
	} else {
		log.Println("Job failed:", errString)
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, "generic").Inc()
	}
}

// Target adds a Target to the job, returning a JobWithTarget
func (j Job) Target(target string) (JobWithTarget, error) {
	if strings.HasPrefix(target, "gs://") {
		return JobWithTarget{Job: j, TargetBucketAndPrefix: target}, nil
	}
	pdt, err := bqx.ParsePDT(target)
	if err != nil {
		return JobWithTarget{}, err
	}
	return JobWithTarget{Job: j, TargetTable: pdt}, nil
}

// Path returns the GCS path prefix to the job data.
func (j Job) Path() string {
	if len(j.Datatype) > 0 {
		return fmt.Sprintf("gs://%s/%s/%s/%s",
			j.Bucket, j.Experiment, j.Datatype, j.Date.Format("2006/01/02/"))
	}
	return fmt.Sprintf("gs://%s/%s/%s",
		j.Bucket, j.Experiment, j.Date.Format("2006/01/02/"))
}

// Marshal marshals the job to json.
func (j Job) Marshal() []byte {
	b, _ := json.Marshal(j)
	return b
}

func (j Job) String() string {
	return fmt.Sprintf("%s:%s/%s", j.Date.Format("20060102"), j.Experiment, j.Datatype)
}

// Error declarations
var (
	ErrClientIsNil            = errors.New("nil datastore client")
	ErrJobAlreadyExists       = errors.New("job already exists")
	ErrJobNotFound            = errors.New("job not found")
	ErrJobIsObsolete          = errors.New("job is obsolete")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrNotYetImplemented      = errors.New("not yet implemented")
	ErrNoChange               = errors.New("no change since last save")
)

// State types are used for the Status.State values
// This is intended to enforce type safety, but compiler accepts string assignment.  8-(
type State string

// State values
// TODO - should we allow different states for different datatypes?
// In principle, a state could be an object that includes available transitions,
// in which case we would want each datatype to have potentially different transition
// details, e.g. the dedup query.
const (
	Init          State = "init"
	Parsing       State = "parsing"
	ParseError    State = "parseError"
	ParseComplete State = "postProcessing" // Ready for post processing, but not started yet.
	Stabilizing   State = "stabilizing"
	Deduplicating State = "deduplicating"
	Joining       State = "joining"
	Finishing     State = "finishing"
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
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	StartTime    time.Time // Time the job was initialized.
	UpdateTime   time.Time // Time of last update.
	UpdateDetail string    // Note from last update
	UpdateCount  int       // Number of updates

	State               State     // String defining the current state.
	LastStateChangeTime time.Time // Used for computing time in state.
	LastError           string    // The most recent error encountered.

	// Note that these are not persisted
	errors []string // all errors related to the job.
}

func (s Status) String() string {
	if len(s.LastError) > 0 {
		return fmt.Sprintf("%s %s (%s) %s",
			s.UpdateTime.Format("01/02~15:04:05"),
			s.State,
			s.UpdateDetail,
			s.LastError)
	}
	return fmt.Sprintf("%s %s (%s)",
		s.UpdateTime.Format("01/02~15:04:05"),
		s.State,
		s.UpdateDetail)
}

func (s Status) isDone() bool {
	return s.State == Complete
}

// Elapsed returns the elapsed time of the Job, rounded to nearest second.
func (s Status) Elapsed() time.Duration {
	return s.UpdateTime.Sub(s.StartTime).Round(time.Second)
}

// NewStatus creates a new Status with provided parameters.
func NewStatus() Status {
	return Status{
		State:     Init,
		errors:    make([]string, 0, 1),
		StartTime: time.Now(),
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

var jobsTemplate = template.Must(template.New("").Parse(
	fmt.Sprintf(`
	<h1>{{.Title}}</h1>
	<style>
	table, th, td {
	  border: 2px solid black;
	}
	</style>
	<table style="width:100%%">
		<tr>
			<th> Job </th>
			<th> Elapsed </th>
			<th> UpdateTime </th>
			<th> State </th>
			<th> Detail </th>
			<th> Updates </th>
			<th> Error </th>
		</tr>
	    {{range .Jobs}}
		<tr>
			<td> {{.Job}} </td>
			<td> {{.Status.Elapsed}} </td>
			<td> {{.Status.UpdateTime.Format "01/02~15:04:05"}} </td>
			<td {{ if or (eq .Status.State "%s") (eq .Status.State "%s")}}
					style="color: red;"
					{{ else }}{{ end }}>
			  {{.Status.State}} </td>
			<td> {{.Status.UpdateDetail}} </td>
			<td> {{.Status.UpdateCount}} </td>
			<td> {{.Status.LastError}} </td>
		</tr>
	    {{end}}
	</table>`, Init, ParseComplete)))

// WriteHTML writes a table containing the jobs and status.
func (jobs JobMap) WriteHTML(w io.Writer) error {
	type Pair struct {
		Job    Job
		Status Status
	}
	type JobRep struct {
		Title string
		Jobs  []Pair
	}
	pairs := make([]Pair, 0, len(jobs))
	for j := range jobs {
		pairs = append(pairs, Pair{Job: j, Status: jobs[j]})
	}
	// Order by age.
	// TODO - color code by how recently there has been an update.  We generally
	// expect some update every 5 to 10 minutes.
	sort.Slice(pairs,
		func(i, j int) bool {
			return pairs[i].Status.StartTime.Before(pairs[j].Status.StartTime)
		})
	jr := JobRep{Title: "Jobs", Jobs: pairs}

	err := jobsTemplate.Execute(w, jr)
	if err != nil {
		log.Println(err)
	}
	return err
}

// saverStruct is used only for saving and loading from datastore.
type saverStruct struct {
	SaveTime time.Time
	LastInit Job
	// Jobs is encoded as json, because datastore doesn't handle maps.
	Jobs []byte `datastore:",noindex"`
}

func loadFromDatastore(ctx context.Context, client dsiface.Client, key *datastore.Key) (saverStruct, error) {
	state := saverStruct{Jobs: make([]byte, 0)}
	if client == nil {
		return state, ErrClientIsNil
	}

	err := client.Get(ctx, key, &state) // This should error?
	return state, err
}

// loadJobMap loads the persisted map of jobs in flight.
func loadJobMap(ctx context.Context, client dsiface.Client, key *datastore.Key) (JobMap, Job, error) {
	state, err := loadFromDatastore(ctx, client, key)
	if err != nil {
		return nil, Job{}, err
	}

	jobMap := make(JobMap, 100)
	log.Println("Unmarshalling", len(state.Jobs))
	err = json.Unmarshal(state.Jobs, &jobMap)
	if err != nil {
		return nil, Job{}, err
	}
	return jobMap, state.LastInit, nil
}
