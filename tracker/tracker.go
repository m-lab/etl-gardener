// Package tracker tracks status of all jobs, and handles persistence.
//
// Concurrency properties:
//  1. The job map is protected by a Mutex, but lock is only required
//     to get a copy or set the Status value, so there is minimal
//     contention.
//  2. Status objects are persisted to a Saver by a separate
//     goroutine that periodically updates any modified Status objects.
//     The Status's updatetime is used to determine whether it needs
//     to be saved.
package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"html/template"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"

	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/go/logx"
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
	Complete      State = "complete"
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

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key
	ticker *time.Ticker

	// The lock should be held whenever accessing the jobs JobMap
	lock         sync.Mutex
	lastModified time.Time

	// These are the stored values.
	lastJob Job    // The last job that was added/initialized.
	jobs    JobMap // Map from Job to Status.

	// Time after which stale job should be ignored or replaced.
	expirationTime time.Duration
	// Delay before removing Complete jobs.
	cleanupDelay time.Duration
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(
	ctx context.Context,
	client dsiface.Client, key *datastore.Key,
	saveInterval time.Duration, expirationTime time.Duration, cleanupDelay time.Duration) (*Tracker, error) {

	jobMap, lastJob, err := loadJobMap(ctx, client, key)
	if err != nil {
		log.Println(err, key)
		jobMap = make(JobMap, 100)
	}
	for j := range jobMap {
		metrics.StartedCount.WithLabelValues(j.Experiment, j.Datatype).Inc()
		metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype).Inc()
	}
	t := Tracker{
		client: client, dsKey: key, lastModified: time.Now(),
		lastJob: lastJob, jobs: jobMap,
		expirationTime: expirationTime, cleanupDelay: cleanupDelay}
	if client != nil && saveInterval > 0 {
		t.saveEvery(saveInterval)
	}
	return &t, nil
}

// NumJobs returns the number of jobs in flight.  This includes
// jobs in "Complete" state that have not been removed from saver.
func (tr *Tracker) NumJobs() int {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return len(tr.jobs)
}

// NumFailed returns the number of failed jobs.
func (tr *Tracker) NumFailed() int {
	jobs, _, _ := tr.GetState()
	counts := make(map[State]int, 20)

	for _, s := range jobs {
		counts[s.State]++
	}
	return counts[Failed]
}

// Sync snapshots the full job state and saves it to the datastore client IFF it has changed.
// Returns time last saved, which may or may not be updated.
func (tr *Tracker) Sync(lastSave time.Time) (time.Time, error) {
	jobs, lastInit, lastMod := tr.GetState()
	if lastMod.Before(lastSave) {
		logx.Debug.Println("Skipping save", lastMod, lastSave)
		return lastSave, nil
	}

	jsonJobs, err := jobs.MarshalJSON()
	if err != nil {
		return lastSave, err
	}

	// Save the full state.
	lastTry := time.Now()
	state := saverStruct{time.Now(), lastInit, jsonJobs}
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = tr.client.Put(ctx, tr.dsKey, &state)

	if err != nil {
		return lastSave, err
	}
	return lastTry, nil
}

func (tr *Tracker) saveEvery(interval time.Duration) {
	tr.ticker = time.NewTicker(interval)
	go func() {
		lastSave := time.Time{}
		for range tr.ticker.C {
			var err error
			lastSave, err = tr.Sync(lastSave)
			if err != nil {
				log.Println(err)
			}
		}
	}()
}

// GetStatus retrieves the status of an existing job.
func (tr *Tracker) GetStatus(job Job) (Status, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return Status{}, ErrJobNotFound
	}
	return status, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists.
func (tr *Tracker) AddJob(job Job) error {
	status := NewStatus()
	status.UpdateTime = time.Now()
	status.LastStateChangeTime = time.Now()

	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if ok {
		return ErrJobAlreadyExists
	}

	tr.lastJob = job
	tr.lastModified = time.Now()
	metrics.StartedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()
	// TODO - should call this JobsInFlight, to avoid confusion with Tasks in parser.
	metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype).Inc()
	tr.jobs[job] = status
	metrics.StateDate.WithLabelValues(job.Experiment, job.Datatype, string(status.State)).Set(float64(job.Date.Unix()))
	return nil
}

// UpdateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) UpdateJob(job Job, state Status) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}

	tr.lastModified = time.Now()
	if state.isDone() {
		metrics.CompletedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()
		metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype).Dec()
		if tr.cleanupDelay == 0 {
			delete(tr.jobs, job)
		}
	} else {
		tr.jobs[job] = state
	}
	return nil
}

// SetStatus updates a job's state in memory.
func (tr *Tracker) SetStatus(job Job, newState State, detail string) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	if newState != status.State {
		timeInState := time.Since(status.LastStateChangeTime)
		metrics.StateTimeHistogram.WithLabelValues(job.Experiment, job.Datatype, string(status.State)).Observe(timeInState.Seconds())
		metrics.StateDate.WithLabelValues(job.Experiment, job.Datatype, string(status.State)).Set(float64(job.Date.Unix()))
		status.LastStateChangeTime = time.Now()
	}
	status.State = newState
	status.UpdateTime = time.Now()
	if detail != "-" {
		status.UpdateDetail = detail
	}
	if newState == ParseComplete {
		// TODO enable this once we have file or byte counts.
		// Update the metrics, even if there is an error, since the files were submitted to the queue already.
		// metrics.FilesPerDateHistogram.WithLabelValues(job.Datatype, strconv.Itoa(job.Date.Year())).Observe(float64(fileCount))
		// metrics.BytesPerDateHistogram.WithLabelValues(t.Experiment, strconv.Itoa(t.Date.Year())).Observe(float64(byteCount))
	}
	status.UpdateCount++
	return tr.UpdateJob(job, status)
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(job Job) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.HeartbeatTime = time.Now()
	return tr.UpdateJob(job, status)
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(job Job, errString string) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.UpdateTime = time.Now()
	status.LastError = errString
	// For now, we set state to failed.  We may want something different in future.
	status.State = Failed
	job.failureMetric(errString)

	timeInState := time.Since(status.LastStateChangeTime)
	metrics.StateTimeHistogram.WithLabelValues(job.Experiment, job.Datatype, string(status.State)).Observe(timeInState.Seconds())
	metrics.StateDate.WithLabelValues(job.Experiment, job.Datatype, string(status.State)).Set(float64(job.Date.Unix()))

	status.errors = append(status.errors, errString)
	return tr.UpdateJob(job, status)
}

// GetState returns the full job map, last initialized Job, and last mod time.
// It also cleans up any expired jobs from the tracker.
func (tr *Tracker) GetState() (JobMap, Job, time.Time) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	m := make(JobMap, len(tr.jobs))
	for j, s := range tr.jobs {
		if (tr.expirationTime > 0 && time.Since(s.UpdateTime) > tr.expirationTime) ||
			s.isDone() && time.Since(s.UpdateTime) > tr.cleanupDelay {
			// Remove any obsolete jobs.
			metrics.CompletedCount.WithLabelValues(j.Experiment, j.Datatype).Inc()
			metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype).Dec()
			log.Println("Deleting stale job", j)
			tr.lastModified = time.Now()
			delete(tr.jobs, j)
		} else {
			m[j] = s
		}
	}
	return m, tr.lastJob, tr.lastModified
}

// WriteHTMLStatusTo writes out the status of all jobs to the html writer.
func (tr *Tracker) WriteHTMLStatusTo(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// TODO - add the lastInit job.
	jobs, _, _ := tr.GetState()

	fmt.Fprint(w, "<div>Tracker State</div>\n")

	return jobs.WriteHTML(w)
}

// LastJob returns the last Job successfully added with AddJob
func (tr *Tracker) LastJob() Job {
	return tr.lastJob
}
