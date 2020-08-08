// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"context"
	"errors"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

// ErrMoreJSON is returned when response from gardener has unknown fields.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

// ErrNilParameter is returned on disallowed nil parameter.
var ErrNilParameter = errors.New("nil parameter not allowed")

type jobAdder interface {
	AddJob(job tracker.Job) error
	LastJob() tracker.Job // temporary
}

// YesterdaySource provides pending jobs for yesterday's data.
// When the scheduled delay has passed, it cycles through all the
// job specs, then advances to the next date.
type YesterdaySource struct {
	saver persistence.Saver

	jobSpecs  []tracker.JobWithTarget // The job prefixes to be iterated through.
	Date      time.Time               // The next "yesterday" date to be processed.
	delay     time.Duration           // time after UTC to process yesterday.
	nextIndex int
}

// nextJob returns a yesterday Job if appropriate
// Not thread-safe.
func (y *YesterdaySource) nextJob(ctx context.Context) *tracker.JobWithTarget {
	// Defer until "delay" after midnight next day.
	if time.Since(y.Date) < 24*time.Hour+y.delay {
		return nil
	}

	// Copy the jobspec and set the date.
	job := y.jobSpecs[y.nextIndex]
	job.Date = y.Date

	// Advance to the next jobSpec for next call.
	y.nextIndex++
	// When we have dispatched all jobs, advance yesterdayDate to next day
	// and reset the index.
	if y.nextIndex >= len(y.jobSpecs) {
		y.nextIndex = 0
		y.Date = y.Date.AddDate(0, 0, 1).UTC().Truncate(24 * time.Hour)

		ctx, cf := context.WithTimeout(ctx, 5*time.Second)
		defer cf()
		log.Println("Saving", y.GetName(), y.GetKind(), y.Date.Format("2006-01-02"))
		err := y.saver.Save(ctx, y)
		if err != nil {
			log.Println(err)
		}
	}

	return &job
}

func initYesterday(ctx context.Context, saver persistence.Saver, delay time.Duration, specs []tracker.JobWithTarget) (*YesterdaySource, error) {
	if saver == nil {
		return nil, ErrNilParameter
	}
	// This is the fallback start date.
	date := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)

	src := YesterdaySource{
		saver:     saver,
		jobSpecs:  specs,
		Date:      date,
		delay:     delay,
		nextIndex: 0,
	}

	// Recover the date from datastore.
	ctx, cf := context.WithTimeout(ctx, 5*time.Second)
	defer cf()
	err := saver.Fetch(ctx, &src)
	if err != nil {
		log.Println(err)
	}

	log.Println("Yesterday starting at", src.Date)
	return &src, nil
}

// GetName implements StateObject.GetName
func (y YesterdaySource) GetName() string {
	return "singleton" // There is only one job service.
}

// GetKind implements StateObject.GetKind
func (y YesterdaySource) GetKind() string {
	return reflect.TypeOf(y).String()
}

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	jobSpecs []tracker.JobWithTarget // The job prefixes to be iterated through.

	saver persistence.Saver // This injected to implement persistence.

	startDate time.Time // The date to restart at.

	// Optional jobAdder to add jobs to, e.g. a Tracker
	jobAdder jobAdder

	// Storage client used to get source lists.
	sClient stiface.Client

	// All fields above are const after initialization.
	// All fields below are protected by *lock*
	lock *sync.Mutex

	// The Date is exported for persistence.  It is the only field
	// that is recovered after restart.  All others are injected
	// from config.
	Date      time.Time // The date currently being dispatched.
	nextIndex int       // index of TypeSource to dispatch next.

	yesterday *YesterdaySource // Provides jobs for high priority yesterday
}

func (svc *Service) advanceDate() {
	date := svc.Date.UTC().AddDate(0, 0, 1).Truncate(24 * time.Hour)
	// Start over when we reach yesterday.
	if time.Since(date) < 36*time.Hour {
		date = svc.startDate
	}
	svc.Date = date
	svc.nextIndex = 0
}

// NextJob returns a tracker.Job to dispatch.
func (svc *Service) NextJob(ctx context.Context) tracker.JobWithTarget {
	svc.lock.Lock()
	defer svc.lock.Unlock()

	// Check whether there is yesterday work to do.
	if j := svc.yesterday.nextJob(ctx); j != nil {
		log.Println("Yesterday job:", j.Job)
		return *j
	}

	job := svc.jobSpecs[svc.nextIndex]
	job.Date = svc.Date
	svc.nextIndex++

	if svc.nextIndex >= len(svc.jobSpecs) {
		svc.advanceDate()
		// Note that this will block other calls to NextJob
		ctx, cf := context.WithTimeout(ctx, 5*time.Second)
		defer cf()
		log.Println("Saving", svc.GetName(), svc.GetKind(), svc.Date.Format("2006-01-02"))
		err := svc.saver.Save(ctx, svc)
		if err != nil {
			log.Println(err)
		}
	}
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
	job := svc.NextJob(req.Context())

	// Check whether there are any files
	// TODO - perhaps actually make and cache a list of all the files?
	var files []string
	var err error
	if svc.sClient != nil {
		files, _, err = job.Job.PrefixStats(req.Context(), svc.sClient)
		if err != nil {
			log.Println(err)
		}
		if len(files) == 0 {
			log.Println(job, "has no files", job.Bucket)
			resp.WriteHeader(http.StatusInternalServerError)
			_, err = resp.Write([]byte("Job has no files.  Try again."))
			if err != nil {
				log.Println(err)
			}
			return
		}
	}

	err = svc.jobAdder.AddJob(job.Job)
	if err != nil {
		log.Println(err, job)
		resp.WriteHeader(http.StatusInternalServerError)
		_, err = resp.Write([]byte("Job already exists.  Try again."))
		if err != nil {
			log.Println(err)
		}
		return
	}

	log.Printf("Dispatching %s with %d files\n", job.Job, len(files))
	_, err = resp.Write(job.Marshal())
	if err != nil {
		log.Println(err)
		// This should precede the Write(), but the Write failed, so this
		// is likely ok.
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Recover the processing date.
// Not thread-safe - should be called before activating service.
func (svc *Service) recoverDate(ctx context.Context) {
	// Default to tracker.LastJob date.
	// Deprecated
	svc.Date = svc.jobAdder.LastJob().Date

	// try to recover date from saver.
	ctx, cf := context.WithTimeout(ctx, 5*time.Second)
	defer cf()
	err := svc.saver.Fetch(ctx, svc)
	if err != nil {
		log.Println(err, svc)
	}

	// Adjust if Date is too early.
	if svc.Date.Before(svc.startDate) {
		svc.Date = svc.startDate
	}
}

// ErrInvalidStartDate is returned if startDate is time.Time{}
var ErrInvalidStartDate = errors.New("Invalid start date")

// NewJobService creates the default job service.
// Context is used for retrieving state from datastore.
func NewJobService(ctx context.Context, tk jobAdder, startDate time.Time,
	targetBase string, sources []config.SourceConfig,
	saver persistence.Saver,
	statsClient stiface.Client, // May be nil
) (*Service, error) {
	if startDate.Equal(time.Time{}) {
		return nil, ErrInvalidStartDate
	}
	if tk == nil || ctx == nil || saver == nil {
		return nil, ErrNilParameter
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

	yesterday, err := initYesterday(ctx, saver, 10*time.Hour+30*time.Minute, specs)
	if err != nil {
		return nil, err
	}

	svc := Service{
		jobAdder:  tk,
		saver:     saver,
		jobSpecs:  specs,
		startDate: startDate,
		sClient:   statsClient,
		lock:      &sync.Mutex{},
		nextIndex: 0,
		yesterday: yesterday,
	}

	svc.recoverDate(ctx)

	return &svc, nil
}

// ---------- Implement persistence.StateObject -------------

// GetName implements StateObject.GetName
func (svc Service) GetName() string {
	return "singleton" // There is only one job service.
}

// GetKind implements StateObject.GetKind
func (svc Service) GetKind() string {
	return reflect.TypeOf(svc).String()
}
