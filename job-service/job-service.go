// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/client"
	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/tracker"
)

// NextJob is required until etl code migrates to client.NextJob
// DEPRECATED
var NextJob = client.NextJob

// ErrMoreJSON is returned when response from gardener has unknown fields.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

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
}

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
	if svc.nextIndex >= len(svc.jobSpecs) {
		svc.advanceDate()
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
func NewJobService(tk *tracker.Tracker, startDate time.Time,
	targetBase string, sources []config.SourceConfig) (*Service, error) {
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

	resume := startDate

	if tk == nil {
		return &Service{tracker: tk, startDate: startDate, date: resume, nextIndex: 0, jobSpecs: specs}, nil
	}

	// Last job from tracker recovery.  This may be empty Job{} if recovery failed.
	lastJob := tk.LastJob()
	log.Println("Last job was:", lastJob)

	// TODO check for spec bucket change
	if resume.Before(lastJob.Date) {
		// override the resume date if lastJob was later.
		resume = lastJob.Date
	}
	// Ok to start here.  If there are repeated jobs, the job-service will skip
	// them.  If they are already finished, then ok to repeat them, though a little inefficient.
	svc := Service{tracker: tk, startDate: startDate, date: resume, nextIndex: 0, jobSpecs: specs}
	return &svc, nil
}
