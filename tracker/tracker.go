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
	"sync"
	"time"

	"html/template"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"

	"github.com/m-lab/etl-gardener/metrics"
)

// Job describes a reprocessing "Job", which includes
// all data for a particular experiment, type and date.
type Job struct {
	Bucket     string
	Experiment string
	Datatype   string
	Date       time.Time
}

// NewJob creates a new job object.
// NB:  The date will be converted to UTC and truncated to day boundary!
func NewJob(bucket, exp, typ string, date time.Time) Job {
	return Job{Bucket: bucket,
		Experiment: exp,
		Datatype:   typ,
		Date:       date.UTC().Truncate(24 * time.Hour)}
}

// Path returns the GCS path prefix to the job data.
func (j *Job) Path() string {
	if len(j.Datatype) > 0 {
		return fmt.Sprintf("gs://%s/%s/%s/%s",
			j.Bucket, j.Experiment, j.Datatype, j.Date.Format("2006/01/02/"))
	}
	return fmt.Sprintf("gs://%s/%s/%s",
		j.Bucket, j.Experiment, j.Date.Format("2006/01/02/"))
}

// Marshal marshals the job to json.
func (j *Job) Marshal() []byte {
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
)

// State types are used for the Status.State values
// This is intended to enforce type safety, but compiler accepts string assignment.  8-(
type State string

// State values
const (
	Init          State = "init"
	Parsing       State = "parsing"
	ParseError    State = "parseError"
	ParseComplete State = "postProcessing" // Ready for post processing, but not started yet.
	Stabilizing   State = "stabilizing"
	Deduplicating State = "deduplicating"
	Joining       State = "joining"
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

	UpdateTime   time.Time // Time of last update.
	UpdateDetail string    // Note from last update

	State     State  // String defining the current state.
	LastError string // The most recent error encountered.

	// Note that these are not persisted
	errors []string // all errors related to the job.
}

func (s Status) String() string {
	if len(s.LastError) > 0 {
		return fmt.Sprintf("%s %s %s (%s)",
			s.UpdateTime.Format("01/02~15:04:05"),
			s.State, s.LastError,
			s.UpdateDetail)
	}
	return fmt.Sprintf("%s %s (%s)",
		s.UpdateTime.Format("01/02~15:04:05"),
		s.State,
		s.UpdateDetail)
}

func (s Status) isDone() bool {
	return s.State == Complete
}

// NewStatus creates a new Status with provided parameters.
func NewStatus() Status {
	return Status{
		State:  Init,
		errors: make([]string, 0, 1),
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

var jobsTemplate = template.Must(template.New("").Parse(`
	<h1>{{.Title}}</h1>
	<style>
	table, th, td {
	  border: 2px solid black;
	}
	</style>
	<table style="width:80%">
		<tr>
			<th> Job </th>
			<th> State </th>
		</tr>
	    {{range .Jobs}}
		<tr>
			<td> {{.Job}} </td>
			<td> {{.State}} </td>
		</tr>
	    {{end}}
	</table>`))

// WriteHTML writes a table containing the jobs and status.
func (jobs JobMap) WriteHTML(w io.Writer) error {
	type Pair struct {
		Job   Job
		State Status
	}
	type JobRep struct {
		Title string
		Jobs  []Pair
	}
	pairs := make([]Pair, 0, len(jobs))
	for j := range jobs {
		pairs = append(pairs, Pair{Job: j, State: jobs[j]})
	}
	sort.Slice(pairs,
		func(i, j int) bool {
			return pairs[i].State.UpdateTime.Before(pairs[j].State.UpdateTime)
		})
	jr := JobRep{Title: "Jobs", Jobs: pairs}

	return jobsTemplate.Execute(w, jr)
}

// saverStruct is used only for saving and loading from datastore.
type saverStruct struct {
	SaveTime time.Time
	Jobs     []byte `datastore:",noindex"`
}

// loadJobMap loads the persisted map of jobs in flight.
func loadJobMap(ctx context.Context, client dsiface.Client, key *datastore.Key) (JobMap, error) {
	if client == nil {
		return nil, ErrClientIsNil
	}
	state := saverStruct{time.Time{}, make([]byte, 0, 100000)}

	err := client.Get(ctx, key, &state) // This should error?
	if err != nil {
		return nil, err
	}
	jobMap := make(JobMap, 100)
	log.Println("Unmarshalling", len(state.Jobs))
	err = json.Unmarshal(state.Jobs, &jobMap)
	if err != nil {
		return nil, err
	}
	return jobMap, nil
}

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	client dsiface.Client
	dsKey  *datastore.Key
	ticker *time.Ticker

	// The lock should be held whenever accessing the jobs JobMap
	lock sync.Mutex
	jobs JobMap // Map from Job to Status.

	// Time after which stale job should be ignored or replaced.
	expirationTime time.Duration
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(
	ctx context.Context,
	client dsiface.Client, key *datastore.Key,
	saveInterval time.Duration, expirationTime time.Duration) (*Tracker, error) {

	jobMap, err := loadJobMap(ctx, client, key)
	if err != nil {
		log.Println(err, key)
		jobMap = make(JobMap, 100)
	}
	t := Tracker{client: client, dsKey: key, jobs: jobMap, expirationTime: expirationTime}
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

// getJSON creates a json encoding of the job map.
func (tr *Tracker) getJSON() ([]byte, error) {
	jobs := tr.GetAll()
	return json.Marshal(jobs)
}

// Sync snapshots the full job state and saves it to the datastore client.
func (tr *Tracker) Sync() error {
	log.Println("sync")
	bytes, err := tr.getJSON()
	if err != nil {
		return err
	}

	// Save the full state.
	state := saverStruct{time.Now(), bytes}
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = tr.client.Put(ctx, tr.dsKey, &state)

	return err
}

func (tr *Tracker) saveEvery(interval time.Duration) {
	tr.ticker = time.NewTicker(interval)
	go func() {
		for range tr.ticker.C {
			tr.Sync()
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

	tr.lock.Lock()
	defer tr.lock.Unlock()
	_, ok := tr.jobs[job]
	if ok {
		return ErrJobAlreadyExists
	}

	// TODO - should call this JobsInFlight, to avoid confusion with Tasks in parser.
	metrics.TasksInFlight.Inc()
	tr.jobs[job] = status
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

	if state.isDone() {
		delete(tr.jobs, job)
		metrics.TasksInFlight.Dec()
	} else {
		tr.jobs[job] = state
	}
	return nil
}

// SetStatus updates a job's state, and handles persistence.
func (tr *Tracker) SetStatus(job Job, newState State, detail string) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.State = newState
	status.UpdateTime = time.Now()
	status.UpdateDetail = detail
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
	status.errors = append(status.errors, errString)
	return tr.UpdateJob(job, status)
}

// GetAll returns the full job map.
// It also cleans up any expired jobs from the tracker.
func (tr *Tracker) GetAll() JobMap {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	m := make(JobMap, len(tr.jobs))
	for k, v := range tr.jobs {
		if tr.expirationTime > 0 && time.Since(v.UpdateTime) > tr.expirationTime {
			// Remove any obsolete jobs.
			delete(tr.jobs, k)
		} else {
			m[k] = v
		}
	}
	return m
}

// WriteHTMLStatusTo writes out the status of all jobs to the html writer.
func (tr *Tracker) WriteHTMLStatusTo(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	jobs := tr.GetAll()

	fmt.Fprint(w, "<div>Tracker State</div>\n")

	return jobs.WriteHTML(w)
}
