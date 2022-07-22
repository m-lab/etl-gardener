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
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/logx"
)

// Key is a unique identifier for a single tracker Job. Key may be used as a map key.
type Key string

// jobStatusMap and jobStateMap are used by the tracker to save all Jobs and Job
// statuses using a shared job Key.
type jobStatusMap map[Key]Status
type jobStateMap map[Key]Job

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	saver GenericSaver

	// The lock should be held whenever accessing the jobs maps.
	lock         sync.Mutex
	lastModified time.Time

	// These values are the complete tracker job state and statuses. They are
	// periodically saved to external storage. On startup, these values are
	// loaded back so the tracker may resume from last known state.
	lastJob  Job          // The last job that was added/initialized.
	statuses jobStatusMap // statuses contains all tracked Job statuses.
	jobs     jobStateMap  // jobs contains all tracked Jobs.

	// Time after which stale job should be ignored or replaced.
	expirationTime time.Duration
	// Delay before removing Complete jobs.
	cleanupDelay time.Duration
}

type GenericSaver interface {
	Save(src any) error
	Load(dst any) error
}

// saverStructV2 is used to save and load all Job status and state to persistent storage.
type saverStructV2 struct {
	SaveTime time.Time
	Statuses jobStatusMap
	Jobs     jobStateMap
}

// saverStructV1 is used only for saving and loading from persistent storage.
type saverStructV1 struct {
	SaveTime time.Time
	LastInit Job
	Jobs     []byte
}

// TODO(soltesz): delete as soon as possible.
func readSaverStructV1(saver GenericSaver) (time.Time, JobMap, Job, error) {
	state := &saverStructV1{}
	err := saver.Load(state)
	if err != nil {
		return time.Time{}, nil, Job{}, err
	}
	jm, j := loadJobMapFromState(state)
	return state.SaveTime, jm, j, nil
}

// loadJobMapFromState completes unmarshalling a saverStructV1.
func loadJobMapFromState(state *saverStructV1) (JobMap, Job) {
	log.Println("Last save:", state.SaveTime.Format("01/02T15:04"))
	log.Println(string(state.Jobs))

	jobMap := make(JobMap, 100)
	log.Println("Unmarshalling", len(state.Jobs))
	err := json.Unmarshal(state.Jobs, &jobMap)
	if err != nil {
		log.Fatal("loadJobMap failed", err)
	}
	for j, s := range jobMap {
		if len(s.History) < 1 {
			log.Fatalf("Empty State history %+v : %+v\n", j, s)
		}
	}
	return jobMap, state.LastInit
}

func (tr *Tracker) writeSaverStructV2(saver GenericSaver) error {
	statuses := jobStatusMap{} // statuses contains all tracked Job statuses.
	jobs := jobStateMap{}      // jobs contains all tracked Jobs.
	// copy Statuses and Jobs
	for k, j := range tr.jobs {
		statuses[k] = tr.statuses[k]
		jobs[k] = j
	}
	s := &saverStructV2{
		SaveTime: time.Now(),
		Statuses: statuses,
		Jobs:     jobs,
	}
	return saver.Save(s)
}

func readSaverStructV2(saver GenericSaver) (time.Time, jobStateMap, jobStatusMap, error) {
	s := &saverStructV2{}
	err := saver.Load(s)
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	log.Println("loading jobs previously saved from:", s.SaveTime)
	return s.SaveTime, s.Jobs, s.Statuses, nil
}

func loadJobMaps(saverV2, saverV1 GenericSaver) (jobStateMap, jobStatusMap) {
	// Attempt to read both the v1 and v2 saver structs.
	tv1, jobMap, _, err1 := readSaverStructV1(saverV1)
	tv2, jobs, statuses, err2 := readSaverStructV2(saverV2)

	// Only use the v2 data if the last saved time is more recent than the v1 time.
	if tv2.After(tv1) && err2 == nil {
		// This means v2 was saved more recently. So, use the newer data.
		return jobs, statuses
	}

	// At this point, it means the v2 data is not yet available. If we also
	// failed to read from the v1 file, return empty sets.
	if err1 != nil {
		return make(jobStateMap), make(jobStatusMap)
	}

	// Now, v2 was not yet available, but the v1 was read successfully.
	// Transform the v1 JobMap into separate statuses and jobs maps.
	statusesV1 := make(jobStatusMap)
	jobsV1 := make(jobStateMap)
	for j, s := range jobMap {
		statusesV1[j.Key()] = s
		jobsV1[j.Key()] = j
	}
	return jobsV1, statusesV1
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(
	ctx context.Context,
	saverV1 GenericSaver,
	saverV2 GenericSaver,
	saveInterval time.Duration,
	expirationTime time.Duration,
	cleanupDelay time.Duration) (*Tracker, error) {

	// Attempt to load from savers.
	jobs, statuses := loadJobMaps(saverV2, saverV1)

	// Update the metrics for all jobs still in flight or failed.
	for k := range jobs {
		j := jobs[k]
		s := statuses[k]
		if !s.isDone() {
			metrics.StartedCount.WithLabelValues(j.Experiment, j.Datatype).Inc()
			metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype, s.Label()).Inc()
		}
	}

	t := Tracker{
		saver:          saverV2,
		lastModified:   time.Now(),
		jobs:           jobs,
		statuses:       statuses,
		expirationTime: expirationTime, cleanupDelay: cleanupDelay}

	if saverV2 != nil && saveInterval > 0 {
		go t.saveEvery(ctx, saveInterval)
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
		counts[s.State()]++
	}
	return counts[Failed]
}

// Sync snapshots the full job state and saves it to persistent storage IFF it has changed.
// Returns time last saved, which may or may not be updated.
func (tr *Tracker) Sync(ctx context.Context, lastSave time.Time) (time.Time, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	lastMod := tr.lastModified
	if lastMod.Before(lastSave) {
		logx.Debug.Println("Skipping save", lastMod, lastSave)
		return lastSave, nil
	}
	err := tr.writeSaverStructV2(tr.saver)
	if err != nil {
		return time.Time{}, err
	}
	return lastMod, nil
}

func (tr *Tracker) saveEvery(ctx context.Context, interval time.Duration) {
	var err error
	ticker := time.NewTicker(interval)
	lastSave := time.Time{}
	for {
		select {
		case <-ticker.C:
			lastSave, err = tr.Sync(ctx, lastSave)
			if err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// GetStatus retrieves the status of an existing job.
// Note that the returned object is a shallow copy, and the History
// field shares the slice objects with the JobMap.
func (tr *Tracker) GetStatus(key Key) (Status, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.statuses[key]
	if !ok {
		return Status{}, ErrJobNotFound
	}
	return status, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists and is still in flight.
func (tr *Tracker) AddJob(job Job) error {
	status := NewStatus()

	tr.lock.Lock()
	defer tr.lock.Unlock()
	s, ok := tr.statuses[job.Key()]
	if ok {
		if s.isDone() {
			log.Println("Restarting completed job", job)
		} else if s.State() == Failed {
			// If job didn't complete, the InFlight metric needs to be updated.
			metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype, s.Label()).Dec()
			log.Println("Restarting failed job", job)
		} else {
			return ErrJobAlreadyExists
		}
	}

	tr.lastJob = job
	tr.lastModified = time.Now()
	metrics.StartedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()
	tr.statuses[job.Key()] = status
	tr.jobs[job.Key()] = job
	status.updateMetrics(job)
	return nil
}

// UpdateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) UpdateJob(key Key, new Status) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	old, ok := tr.statuses[key]
	if !ok {
		return ErrJobNotFound
	}
	job, ok := tr.jobs[key]
	if !ok {
		return ErrJobNotFound
	}

	if old.State() != new.State() {
		new.updateMetrics(job)
	}

	tr.lastModified = time.Now()
	// When jobs are done, we update stats and may remove them from tracker.
	if new.isDone() {
		metrics.CompletedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()
		metrics.JobsTotal.WithLabelValues(job.Experiment, job.Datatype, job.IsDaily(), "success").Inc()

		// This could be done by GetStatus, but would change behaviors slightly.
		if tr.cleanupDelay == 0 {
			delete(tr.statuses, key)
			delete(tr.jobs, key)
			return nil
		}
	}
	tr.statuses[key] = new
	tr.jobs[key] = job
	return nil
}

// SetDetail updates a job's detail message in memory.
func (tr *Tracker) SetDetail(key Key, detail string) error {
	// NOTE: This is not a deep copy.  Shares the History elements.
	status, err := tr.GetStatus(key)
	if err != nil {
		return err
	}
	status.SetDetail(detail)
	status.UpdateCount++
	return tr.UpdateJob(key, status)
}

// SetStatus updates a job's state in memory.
// It may or may not change the job state.  If it does change state,
// the detail string is applied to the last state, not the new state.
func (tr *Tracker) SetStatus(key Key, state State, detail string) error {
	// NOTE: This is not a deep copy.  Shares the History elements.
	status, err := tr.GetStatus(key)
	if err != nil {
		job := tr.jobs[key]
		metrics.WarningCount.WithLabelValues(job.Experiment, job.Datatype, "NoSuchJob").Inc()
		return err
	}
	last := status.LastStateInfo()
	status.SetDetail(detail)

	if state != last.State {
		status.NewState(state)

		if state == ParseComplete {
			// TODO enable this once we have file or byte counts.
			// Alternatively, incorporate this into the next Action!
			// Update the metrics, even if there is an error, since the files were submitted to the queue already.
			// metrics.FilesPerDateHistogram.WithLabelValues(job.Datatype, strconv.Itoa(job.Date.Year())).Observe(float64(fileCount))
			// metrics.BytesPerDateHistogram.WithLabelValues(t.Experiment, strconv.Itoa(t.Date.Year())).Observe(float64(byteCount))
		}
	}
	status.UpdateCount++
	return tr.UpdateJob(key, status)
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(key Key) error {
	status, err := tr.GetStatus(key)
	if err != nil {
		return err
	}
	status.HeartbeatTime = time.Now()
	return tr.UpdateJob(key, status)
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(key Key, errString string) error {
	status, err := tr.GetStatus(key)
	if err != nil {
		return err
	}
	oldState := status.State()
	job := tr.jobs[key]
	job.failureMetric(oldState, errString)
	status.NewState(Failed)
	// Set the final detail to include the prior state and error message.
	status.SetDetail(fmt.Sprintf("%s: %s", oldState, errString))

	return tr.UpdateJob(key, status)
}

// GetState returns the full job map, last initialized Job, and last mod time.
// It also cleans up any expired jobs from the tracker.
func (tr *Tracker) GetState() (JobMap, Job, time.Time) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	// Construct a JobMap from the jobs and statues maps.
	m := make(JobMap)
	for k, j := range tr.jobs {
		s := tr.statuses[k]
		// Remove any obsolete jobs.
		updateTime := s.DetailTime()
		if (tr.expirationTime > 0 && time.Since(updateTime) > tr.expirationTime) ||
			(s.isDone() && time.Since(updateTime) > tr.cleanupDelay) {
			if !s.isDone() {
				// If job didn't complete, the InFlight metric needs to be updated.
				metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype, s.Label()).Dec()
				log.Println("Deleting stale job", j, time.Since(updateTime), tr.cleanupDelay)
			}
			tr.lastModified = time.Now()
			delete(tr.jobs, k)
			delete(tr.statuses, k)
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
