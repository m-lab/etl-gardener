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
	Copying       State = "copying"
	Cleaning      State = "cleaning"
	Finishing     State = "finishing"
	Failed        State = "failed"
	Complete      State = "complete"
)

// StateInfo describes each state in processing history.
type StateInfo struct {
	State          State
	Start          time.Time
	LastUpdateTime time.Time
	LastUpdate     string // status or error, e.g. last filename in Parsing state.
}

// NewStateInfo returns a properly initialized StateInfo
func NewStateInfo(state State) StateInfo {
	now := time.Now()
	si := StateInfo{State: state, Start: now, LastUpdateTime: now}
	return si
}

// Update changes the update time and detail string (if != "-").
// NOT THREADSAFE.  Caller must control access.
func (si *StateInfo) Update(detail string) {
	si.LastUpdateTime = time.Now()
	if detail != "-" {
		si.LastUpdate = detail
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

// LastUpdate returns the most recent update detail string.
// NOTE: update field for Failed state is the error message, so
// the previous StateInfo is used for LastUpdate.
func (s *Status) LastUpdate() string {
	lsi := s.LastStateInfo()
	if lsi.State == Failed && len(s.History) > 1 {
		lsi = s.History[len(s.History)-2]
	}
	return lsi.LastUpdate
}

// UpdateTime returns the timestamp of the most recent update.
func (s *Status) UpdateTime() time.Time {
	return s.LastStateInfo().LastUpdateTime
}

// LastStateChangeTime returns the start time of the current state.
func (s *Status) LastStateChangeTime() time.Time {
	return s.LastStateInfo().Start
}

// StartTime returns the time that this job was started.
func (s *Status) StartTime() time.Time {
	return s.History[0].Start
}

// UpdateDetail replaces the most recent StateInfo with copy with new detail.
// It returns the previous StateInfo value.
func (s *Status) UpdateDetail(detail string) StateInfo {
	result := s.LastStateInfo()
	if detail != "-" {
		// The History is not deep copied, so we do copy on write
		// to avoid race.
		h := make([]StateInfo, len(s.History), cap(s.History))
		copy(h, s.History)

		last := len(h) - 1
		lsi := &h[last]
		lsi.Update(detail)
		// Replace the entire history
		s.History = h
	}
	return result
}

func (s *Status) Error() string {
	ls := s.LastStateInfo()
	if ls.State == Failed {
		return ls.LastUpdate
	}
	return ""
}

// Update applies the detail as LastUpdate, and transitions to new State
// if different from the previous state.
func (s *Status) Update(state State, detail string) StateInfo {
	old := s.UpdateDetail(detail)
	if s.State() == state {
		return old
	}
	if old.State != state {
		s.History = append(s.History, NewStateInfo(state))
		// For failure, we want to record the error in the final state, too.
		// TODO - deprecate ParseError?
		if state == Failed || state == ParseError {
			s.UpdateDetail(detail)
		}
	}
	return old
}

func (s Status) String() string {
	last := s.LastStateInfo()
	return fmt.Sprintf("%s %s (%s)",
		s.UpdateTime().Format("01/02~15:04:05"),
		last.State,
		last.LastUpdate)
}

func (s *Status) isDone() bool {
	return s.LastStateInfo().State == Complete
}

// Elapsed returns the elapsed time of the Job, rounded to nearest second.
func (s *Status) Elapsed() time.Duration {
	return s.UpdateTime().Sub(s.History[0].Start).Round(time.Second)
}

// NewStatus creates a new Status with provided parameters.
func NewStatus() Status {
	now := time.Now()
	return Status{
		History: []StateInfo{{State: Init, Start: now, LastUpdateTime: now}},
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
			<td> {{.Status.LastUpdate}} </td>
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
