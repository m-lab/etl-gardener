// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	lock sync.Mutex

	// Optional tracker to add jobs to.
	tracker *tracker.Tracker

	startDate time.Time // The date to restart at.
	date      time.Time // The date currently being dispatched.

	jobTypes  []tracker.Job // The job prefixes to be iterated through.
	nextIndex int           // index of TypeSource to dispatch next.
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
func (svc *Service) NextJob() tracker.Job {
	svc.lock.Lock()
	defer svc.lock.Unlock()
	if svc.nextIndex >= len(svc.jobTypes) {
		svc.advanceDate()
	}
	job := svc.jobTypes[svc.nextIndex]
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
		err := svc.tracker.AddJob(job)
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

	_, err := resp.Write(job.Marshal())
	if err != nil {
		log.Println(err)
		// This should precede the Write(), but the Write failed, so this
		// is likely ok.
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// NewJobService creates the default job service.
func NewJobService(tk *tracker.Tracker, startDate time.Time) (*Service, error) {
	types := []tracker.Job{
		// Hack for sandbox only.
		tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "ndt5"},
		tracker.Job{Bucket: "archive-mlab-sandbox", Experiment: "ndt", Datatype: "tcpinfo"},
	}

	start := startDate.UTC().Truncate(24 * time.Hour)
	return &Service{tracker: tk, startDate: start, date: start, jobTypes: types}, nil
}
