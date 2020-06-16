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

	"github.com/m-lab/go/cloud/bqx"

	"github.com/m-lab/etl-gardener/metrics"
)

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
	// Filter is an optional regex to apply to ArchiveURL names
	Filter string `json:",omitempty"`
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

func (j JobWithTarget) String() string {
	return fmt.Sprint(j.Job.String(), j.Filter)
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

func (j Job) failureMetric(state State, errString string) {
	log.Printf("Job failed in state: %s -- %s\n", state, errString)
	errStringLock.Lock()
	defer errStringLock.Unlock()
	if _, ok := errStrings[errString]; ok {
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, errString).Inc()
	} else if len(errStrings) < maxUniqueErrStrings {
		errStrings[errString] = struct{}{}
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, errString).Inc()
	} else {
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
	Copying       State = "copying"
	Finishing     State = "finishing"
	Failed        State = "failed"
	Complete      State = "complete"
)

// StateInfo describes each state in processing history.
type StateInfo struct {
	State      State     // const after creation
	Start      time.Time // const after creation
	DetailTime time.Time
	Detail     string // status or error, e.g. last filename in Parsing state.
}

// newStateInfo returns a properly initialized StateInfo
func newStateInfo(state State) StateInfo {
	now := time.Now()
	si := StateInfo{State: state, Start: now, DetailTime: now}
	return si
}

// setDetail changes the setDetail time and detail string (if != "-").
// NOT THREADSAFE.  Caller must control access.
func (si *StateInfo) setDetail(detail string) {
	si.DetailTime = time.Now()
	if detail != "-" {
		si.Detail = detail
	}
}

// A Status describes the state of a bucket/exp/type/YYYY/MM/DD job.
// Completed jobs are removed from the persistent store.
// Errored jobs are maintained in the persistent store for debugging.
// Status should be updated only by the Tracker, which will
// ensure correct serialization and Saver updates.
type Status struct {
	HeartbeatTime time.Time // Time of last ETL heartbeat.

	UpdateCount int // Number of updates

	// History has shared backing store.  Copy on write is used to avoid
	// changing the underlying StateInfo that is shared by the tracker
	// JobMap and accessed concurrently by other goroutines.
	History []StateInfo
}

// LastStateInfo returns copy of the StateInfo for the most recent state.
func (s *Status) LastStateInfo() StateInfo {
	return s.History[len(s.History)-1]
}

// State returns the job State enum.
func (s *Status) State() State {
	return s.LastStateInfo().State
}

// Detail returns the most recent detail string.
// If the detail is empty, returns the previous state detail.
func (s *Status) Detail() string {
	lsi := s.LastStateInfo()
	if len(lsi.Detail) == 0 && len(s.History) > 1 {
		lsi = s.History[len(s.History)-2]
	}
	return lsi.Detail
}

// DetailTime returns the timestamp of the most recent detail update.
func (s *Status) DetailTime() time.Time {
	return s.LastStateInfo().DetailTime
}

// StateChangeTime returns the start time of the current state.
func (s *Status) StateChangeTime() time.Time {
	return s.LastStateInfo().Start
}

// StartTime returns the time that this job was started.
func (s *Status) StartTime() time.Time {
	return s.History[0].Start
}

// SetDetail replaces the most recent StateInfo with copy containing new detail.
// It returns the previous StateInfo value.
func (s *Status) SetDetail(detail string) StateInfo {
	result := s.LastStateInfo()
	if detail != "-" {
		// The History is not deep copied, so we do copy on write
		// to avoid race.
		h := make([]StateInfo, len(s.History), cap(s.History))
		copy(h, s.History)

		last := len(h) - 1
		lsi := &h[last]
		lsi.setDetail(detail)
		// Replace the entire history
		s.History = h
	}
	return result
}

func (s *Status) Error() string {
	ls := s.LastStateInfo()
	if ls.State == Failed {
		return ls.Detail
	}
	return ""
}

// UpdateMetrics handles the StateTimeHistogram and StateDate metric updates.
func (s *Status) updateMetrics(job Job) {
	new := s.LastStateInfo()
	// Update the StateDate metric for new state
	metrics.StateDate.WithLabelValues(job.Experiment, job.Datatype, string(new.State)).Set(float64(job.Date.Unix()))

	if len(s.History) > 1 {
		// Track the time in old state
		old := s.History[len(s.History)-2]
		timeInState := time.Since(old.Start)
		if new.State != Init {
			metrics.StateTimeHistogram.WithLabelValues(job.Experiment, job.Datatype, string(old.State)).Observe(timeInState.Seconds())
		}
	}
}

// NewState adds a new StateInfo to the status.
// If state is unchanged, it just logs a warning.
// Returns the previous StateInfo
func (s *Status) NewState(state State) StateInfo {
	old := s.LastStateInfo()
	if old.State == state {
		log.Println("Warning - same state")
	} else {
		s.History = append(s.History, newStateInfo(state))
	}
	return old
}

func (s Status) String() string {
	last := s.LastStateInfo()
	return fmt.Sprintf("%s %s (%s)",
		s.DetailTime().Format("01/02~15:04:05"),
		last.State,
		last.Detail)
}

func (s *Status) isDone() bool {
	return s.LastStateInfo().State == Complete
}

// Elapsed returns the elapsed time of the Job, rounded to nearest second.
func (s *Status) Elapsed() time.Duration {
	return s.DetailTime().Sub(s.History[0].Start).Round(time.Second)
}

// NewStatus creates a new Status with provided parameters.
func NewStatus() Status {
	now := time.Now()
	return Status{
		History: []StateInfo{{State: Init, Start: now, DetailTime: now}},
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

	log.Printf("Unmarshalling %d job/status pairs.\n", len(pairs))
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
			<th> Update Time </th>
			<th> State </th>
			<th> Detail </th>
			<th> Updates </th>
			<th> Error </th>
		</tr>
	    {{range .Jobs}}
		<tr>
			<td> {{.Job}} </td>
			<td> {{.Status.Elapsed}} </td>
			<td> {{.Status.DetailTime.Format "01/02~15:04:05"}} </td>
			<td {{ if or (eq .Status.State "%s") (eq .Status.State "%s")}}
					style="color: red;"
					{{ else }}{{ end }}>
			  {{.Status.State}} </td>
			<td> {{.Status.Detail}} </td>
			<td> {{.Status.UpdateCount}} </td>
			<td> {{.Status.Error}} </td>
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
			return pairs[i].Status.StartTime().Before(pairs[j].Status.StartTime())
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
	log.Println("Last save:", state.SaveTime.Format("01/02T15:04"))
	log.Println(string(state.Jobs))

	jobMap := make(JobMap, 100)
	log.Println("Unmarshalling", len(state.Jobs))
	err = json.Unmarshal(state.Jobs, &jobMap)
	if err != nil {
		log.Fatal("loadJobMap failed", err)
	}
	for j, s := range jobMap {
		if len(s.History) < 1 {
			log.Fatalf("Empty State history %+v : %+v\n", j, s)
		}
	}
	return jobMap, state.LastInit, nil

}
