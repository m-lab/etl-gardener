// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/cloud/ds"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/tracker"
)

// ErrMoreJSON is returned when response from gardener has unknown fields.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

// yesterdaySource provides pending jobs for yesterday's data.
// When the scheduled delay has passed, it cycles through all the
// job specs, then advances to the next date.
type yesterdaySource struct {
	jobSpecs  []tracker.JobWithTarget // The job prefixes to be iterated through.
	date      time.Time               // The next "yesterday" date to be processed.
	delay     time.Duration           // time after UTC to process yesterday.
	nextIndex int
}

// nextJob returns a yesterday Job if appropriate
// Not thread-safe.
func (y *yesterdaySource) nextJob() *tracker.JobWithTarget {
	// Defer until "delay" after midnight next day.
	if time.Since(y.date) < 24*time.Hour+y.delay {
		return nil
	}

	// Copy the jobspec and set the date.
	job := y.jobSpecs[y.nextIndex]
	job.Date = y.date

	// Advance to the next jobSpec for next call.
	y.nextIndex++
	// When we have dispatched all jobs, advance yesterdayDate to next day
	// and reset the index.
	if y.nextIndex >= len(y.jobSpecs) {
		y.nextIndex = 0
		y.date = y.date.AddDate(0, 0, 1).UTC().Truncate(24 * time.Hour)
	}

	return &job
}

func initYesterday(delay time.Duration, specs []tracker.JobWithTarget, date time.Time) *yesterdaySource {
	// If it is less than 3 hours since trigger time, then start with
	// day before today to minimize risk of missing the yesterday processing.
	// Otherwise start with today.
	date = date.UTC().Truncate(24 * time.Hour)
	log.Println("Yesterday:", date)
	return &yesterdaySource{
		jobSpecs:  specs,
		date:      date,
		delay:     delay,
		nextIndex: 0,
	}
}

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	lock sync.Mutex

	// Optional tracker to add jobs to.
	tracker *tracker.Tracker

	startDate time.Time // The date to restart at.
	date      time.Time // The date currently being dispatched.

	jobSpecs  []tracker.JobWithTarget // The job prefixes to be iterated through.
	nextIndex int                     // index of TypeSource to dispatch next.

	yesterday *yesterdaySource // Provides jobs for high priority yesterday

	dsSaver *saver
}

// Not thread-safe.  Caller must hold svc.lock.
func (svc *Service) advanceDate() {
	date := svc.date.UTC().Add(24 * time.Hour).Truncate(24 * time.Hour)
	if time.Since(date) < 36*time.Hour {
		date = svc.startDate
	}
	svc.date = date
	svc.nextIndex = 0
}

// NextJob returns a tracker.Job to dispatch.
func (svc *Service) NextJob() tracker.JobWithTarget {
	svc.lock.Lock()
	defer svc.lock.Unlock()

	// Check whether there is yesterday work to do.
	if j := svc.yesterday.nextJob(); j != nil {
		log.Println("Yesterday job:", j.Job)
		return *j
	}

	if svc.nextIndex >= len(svc.jobSpecs) {
		svc.advanceDate()
		svc.snapshot()
	}
	job := svc.jobSpecs[svc.nextIndex]
	job.Date = svc.date
	svc.nextIndex++
	return job
}

// JobHandler handle requests for new jobs.
// TODO - should update tracker instance.
func (svc *Service) JobHandler(resp http.ResponseWriter, req *http.Request) {
	// Must be a post because it changes state.
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	job := svc.NextJob()
	if svc.tracker != nil {
		err := svc.tracker.AddJob(job.Job)
		if err != nil {
			log.Println(err, job)
			resp.WriteHeader(http.StatusInternalServerError)
			_, err = resp.Write([]byte("Job already exists.  Try again."))
			if err != nil {
				log.Println(err)
			}
			return
		}
	}

	log.Println("Dispatching", job.Job)
	_, err := resp.Write(job.Marshal())
	if err != nil {
		log.Println(err)
		// This should precede the Write(), but the Write failed, so this
		// is likely ok.
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// ErrInvalidStartDate is returned if startDate is time.Time{}
var ErrInvalidStartDate = errors.New("Invalid start date")

// NewJobService creates the default job service.
func NewJobService(
	dsCtx ds.Context,
	tk *tracker.Tracker, startDate time.Time,
	targetBase string, sources []config.SourceConfig,
) (*Service, error) {
	if startDate.Equal(time.Time{}) {
		return nil, ErrInvalidStartDate
	}
	// The service cycles through the jobSpecs.  Each spec is a job (bucket/exp/type) and a target GCS bucket or BQ table.
	specs := make([]tracker.JobWithTarget, 0)
	for _, s := range sources {
		log.Println(s)
		job := tracker.Job{
			Bucket:     s.Bucket,
			Experiment: s.Experiment,
			Datatype:   s.Datatype,
			Filter:     s.Filter,
			Date:       time.Time{}, // This is not used.
		}
		// TODO - handle gs:// targets
		jt, err := job.Target(targetBase + "." + s.Target)
		if err != nil {
			log.Println(err, targetBase+s.Target)
			continue
		}
		specs = append(specs, jt)
	}
	if len(specs) < 1 {
		log.Fatal("No jobs specified")
	}

	dsSaver := saver{
		Context: dsCtx,
	}
	err := dsSaver.loadFromDatastore()
	if err == nil {
		yesterday := initYesterday(6*time.Hour, specs, dsSaver.YesterdayDate)
		return &Service{
			tracker:   tk,
			startDate: startDate,
			date:      dsSaver.CurrentDate,
			yesterday: yesterday,
			jobSpecs:  specs}, nil
	}

	y := time.Now().UTC().Truncate(24 * time.Hour)
	if time.Since(y) < 6*time.Hour {
		y = y.AddDate(0, 0, -1)
	}
	yesterday := initYesterday(6*time.Hour, specs, y)

	// Deprecated - remove this code soon.
	resume := startDate

	if tk == nil {
		return &Service{tracker: tk,
			dsSaver: &dsSaver,

			startDate: startDate,
			date:      resume,

			yesterday: yesterday,
			jobSpecs:  specs}, nil
	}

	// Last job from tracker recovery.  This may be empty Job{} if recovery failed.
	lastJob := tk.LastJob()
	log.Println("Last job was:", lastJob)

	// TODO check for spec bucket change
	if resume.Before(lastJob.Date) {
		// override the resume date if lastJob was later.
		resume = lastJob.Date
	}

	return &Service{
		tracker: tk,
		dsSaver: &dsSaver,

		startDate: startDate,
		date:      resume,
		yesterday: yesterday,
		jobSpecs:  specs}, nil
}

// snapshots the current state for persistence.
// NOT thread-safe
func (svc *Service) snapshot() {
	svc.dsSaver.SaveTime = time.Now()
	svc.dsSaver.CurrentDate = svc.date
	svc.dsSaver.YesterdayDate = svc.yesterday.date
	cp := svc.dsSaver
	go cp.save()
}

// saver is used only for saving and loading from datastore.
type saver struct {
	SaveTime      time.Time
	CurrentDate   time.Time // Date we should resume processing
	YesterdayDate time.Time // Current YesterdayDate that should be processed at 0600 UTC.

	ds.Context
}

func (ss *saver) save() {
	if ss.Client == nil {
		log.Println("saver not configured")
		return
	}
	ctx, cf := context.WithTimeout(ss.Ctx, 10*time.Second)
	defer cf()
	_, err := ss.Context.Client.Put(ctx, ss.Key, ss)
	if err != nil {
		log.Println(err)
	}
}

func (ss *saver) loadFromDatastore() error {
	if ss.Client == nil {
		return tracker.ErrClientIsNil
	}
	return ss.Client.Get(ss.Ctx, ss.Key, ss)
}
