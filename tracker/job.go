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
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/cloud/gcs"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/timex"
)

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
	// Filter is an optional regex to apply to ArchiveURL names
	// Note that HasFiles does not use this, so ETL may process no files.
	Filter   string          `json:",omitempty"`
	Datasets config.Datasets `json:",omitempty"` // TODO: does this belong here?
}

// TablePartition returns the BigQuery table partition for this Job's Date.
func (j *Job) TablePartition() string {
	return j.Datatype + "$" + j.Date.Format(timex.YYYYMMDD)
}

// JobWithTarget specifies a type/date job, and a destination
// table or GCS prefix
type JobWithTarget struct {
	ID        Key // ID used by gardener & parsers to identify a Job's status and configuration.
	Job       Job
	DailyOnly bool `json:"-"`
	// TODO: enable configuration for parser to target alterate buckets.
}

func (j JobWithTarget) String() string {
	return fmt.Sprint(j.Job.String(), j.Job.Filter)
}

// Marshal marshals the JobWithTarget to json. If the JobWithTarget type ever
// includes fields that cannot be marshalled, then Marshal will panic.
func (j JobWithTarget) Marshal() []byte {
	b, err := json.Marshal(j)
	// NOTE: marshaling a struct with primitive types should never fail.
	rtx.PanicOnError(err, "failed to marshal JobWithTarget: %s", j)
	return b
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
		metrics.JobsTotal.WithLabelValues(j.Experiment, j.Datatype, j.IsDaily(), errString).Inc()
	} else if len(errStrings) < maxUniqueErrStrings {
		errStrings[errString] = struct{}{}
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, errString).Inc()
		metrics.JobsTotal.WithLabelValues(j.Experiment, j.Datatype, j.IsDaily(), errString).Inc()
	} else {
		metrics.FailCount.WithLabelValues(j.Experiment, j.Datatype, "generic").Inc()
		metrics.JobsTotal.WithLabelValues(j.Experiment, j.Datatype, j.IsDaily(), "generic").Inc()
	}
}

// Path returns the GCS path prefix to the job data.
func (j Job) Path() string {
	if len(j.Datatype) > 0 {
		return fmt.Sprintf("gs://%s/%s/%s/%s/",
			j.Bucket, j.Experiment, j.Datatype, j.Date.Format(timex.YYYYMMDDWithSlash))
	}
	return fmt.Sprintf("gs://%s/%s/%s/",
		j.Bucket, j.Experiment, j.Date.Format(timex.YYYYMMDDWithSlash))
}

func (j Job) String() string {
	return fmt.Sprintf("%s:%s/%s", j.Date.Format(timex.YYYYMMDD), j.Experiment, j.Datatype)
}

// Prefix returns the path prefix for a job, not including the gs://bucket-name/.
func (j Job) Prefix() (string, error) {
	p := j.Path()
	parts := strings.SplitN(p, "/", 4)
	if len(parts) != 4 || parts[0] != "gs:" || len(parts[3]) == 0 {
		return "", errors.New("Bad GCS path")
	}
	return parts[3], nil
}

// PrefixStats queries storage and gets a list of all file objects.
func (j Job) PrefixStats(ctx context.Context, sClient stiface.Client) ([]*storage.ObjectAttrs, int64, error) {
	bh, err := gcs.GetBucket(ctx, sClient, j.Bucket)
	if err != nil {
		return []*storage.ObjectAttrs{}, 0, err
	}
	prefix, err := j.Prefix()
	log.Println(prefix)
	if err != nil {
		return []*storage.ObjectAttrs{}, 0, err
	}
	return bh.GetFilesSince(ctx, prefix, nil, time.Time{})
}

// HasFiles queries storage and gets a list of all file objects.
func (j Job) HasFiles(ctx context.Context, sClient stiface.Client) (bool, error) {
	bh, err := gcs.GetBucket(ctx, sClient, j.Bucket)
	if err != nil {
		return false, err
	}
	prefix, err := j.Prefix()
	log.Println(prefix)
	if err != nil {
		return false, err
	}
	return bh.HasFiles(ctx, prefix)
}

// IsDaily returns a string representing whether the job is a Daily job (e.g., job.Date = yesterday).
func (j Job) IsDaily() string {
	isDaily := j.Date.Equal(YesterdayDate())
	return strconv.FormatBool(isDaily)
}

// Key returns a Job unique identifier (within the set of all jobs), suitable for use as a map key.
func (j Job) Key() Key {
	// TODO(soltesz): include target dataset in key to allow different experiment/datatype combinations without conflict.
	return Key(fmt.Sprintf("%s/%s/%s/%s", j.Bucket, j.Experiment, j.Datatype, j.Date.Format(timex.YYYYMMDD)))
}

// YesterdayDate returns the date for the daily job (e.g, yesterday UTC).
func YesterdayDate() time.Time {
	return time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
}

/////////////////////////////////////////////////////////////
//                      State                              //
/////////////////////////////////////////////////////////////

// Error declarations
var (
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
	Loading       State = "loading"
	Deduplicating State = "deduplicating"
	Copying       State = "copying"
	Joining       State = "joining"
	Deleting      State = "deleting"
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

// Prev returns the penultimate job State enum, or Init
func (s *Status) Prev() State {
	if len(s.History) < 2 {
		return Init
	}
	old := s.History[len(s.History)-2]
	return old.State
}

// Label provides a state label for metrics.
// If the final state is Failed, it composes the previous and final state, e.g. Loading-Failed
func (s *Status) Label() string {
	if s.State() == Failed {
		return strings.Join([]string{string(s.Prev()), string(Failed)}, "-")
	}
	return string(s.State())
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
// Not thread-safe.  Caller must hold the job's lock.
func (s *Status) updateMetrics(job Job) {
	new := s.LastStateInfo()
	// Update the StateDate metric for new state
	metrics.StateDate.WithLabelValues(job.Experiment, job.Datatype, string(new.State)).Set(float64(job.Date.Unix()))

	if len(s.History) > 1 {
		// Track the time in old state
		old := s.History[len(s.History)-2]
		timeInState := time.Since(old.Start)
		metrics.StateTimeHistogram.WithLabelValues(job.Experiment, job.Datatype, string(old.State)).Observe(timeInState.Seconds())
		// old state will never be Failed, so the label is just the old.State.
		metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype, string(old.State)).Dec()
	}

	// Use s.Label() which takes into account whether the state is Failed.
	metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype, s.Label()).Inc()
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
type JobMap map[Job]Status

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
