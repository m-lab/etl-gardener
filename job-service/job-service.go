// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/tracker"
)

// ErrMoreJSON is returned when response from gardener has unknown fields.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

// ErrNoConfiguredJobs is returned when a new Job Service cannot be created
// because there are no configured jobs.
var ErrNoConfiguredJobs = errors.New("no configured jobs")

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	// Storage client used to get source lists.
	sClient stiface.Client

	// All fields above are const after initialization.
	// All fields below are protected by *lock*
	lock *sync.Mutex

	dailyJobs *JobIterator // for daily jobs.
	histJobs  *JobIterator // for historical jobs.
}

// NextJob returns a tracker.Job to dispatch.
func (svc *Service) NextJob(ctx context.Context) *tracker.JobWithTarget {
	svc.lock.Lock()
	defer svc.lock.Unlock()

	// Check whether there is yesterday work to do.
	if jt, _ := svc.dailyJobs.Next(); jt != nil {
		log.Println("Yesterday job:", jt.Job)
		return svc.ifHasFiles(ctx, jt)
	}

	// Since some jobs may be configured as dailyOnly, or have no files for a
	// given date, we check for these conditions, and skip the job if
	// appropriate. We try up to histJobs.Len() times to find the next job.
	for attempts := 0; attempts < svc.histJobs.Len(); attempts++ {
		jt, err := svc.histJobs.Next()
		if err != nil {
			log.Println(err)
			continue
		}
		return svc.ifHasFiles(ctx, jt)
	}
	return nil
}

func (svc *Service) ifHasFiles(ctx context.Context, jt *tracker.JobWithTarget) *tracker.JobWithTarget {
	if svc.sClient != nil {
		ok, err := jt.Job.HasFiles(ctx, svc.sClient)
		if err != nil {
			log.Println(err)
		}
		if !ok {
			log.Println(jt, "has no files", jt.Job.Bucket)
			return nil
		}
	}
	return jt
}

// ErrInvalidStartDate is returned if startDate is time.Time{}
var ErrInvalidStartDate = errors.New("invalid start date")

// NewJobService creates the default job service.
func NewJobService(startDate time.Time,
	sources []config.SourceConfig,
	statsClient stiface.Client, // May be nil
	dailySaver namedSaver,
	histSaver namedSaver,
) (*Service, error) {
	if startDate.Equal(time.Time{}) {
		return nil, ErrInvalidStartDate
	}

	// The service cycles through the jobSpecs.  Each spec is a job (bucket/exp/type) and a target GCS bucket or BQ table.
	dailySpecs := make([]tracker.JobWithTarget, 0)
	histSpecs := make([]tracker.JobWithTarget, 0)
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
		jt := tracker.JobWithTarget{
			ID:        job.Key(),
			Job:       job,
			DailyOnly: s.DailyOnly,
		}
		dailySpecs = append(dailySpecs, jt)
		if !s.DailyOnly {
			histSpecs = append(histSpecs, jt)
		}
	}
	if len(dailySpecs) == 0 {
		// NOTE: the set of historical specs may be empty.
		return nil, ErrNoConfiguredJobs
	}

	di := NewDailyIterator(10*time.Hour+30*time.Minute, dailySaver)
	hi := NewHistoricalIterator(startDate, histSaver)

	dailyJobs := NewJobIterator(di, dailySpecs)
	histJobs := NewJobIterator(hi, histSpecs)

	svc := Service{
		dailyJobs: dailyJobs,
		histJobs:  histJobs,
		sClient:   statsClient,
		lock:      &sync.Mutex{},
	}

	return &svc, nil
}
