// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
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
func NewJobService(tk *tracker.Tracker, bucket string, startDate time.Time) (*Service, error) {
	types := []tracker.Job{
		// Hack for sandbox only.
		tracker.Job{Bucket: bucket, Experiment: "ndt", Datatype: "ndt5"},
		tracker.Job{Bucket: bucket, Experiment: "ndt", Datatype: "tcpinfo"},
	}

	start := startDate.UTC().Truncate(24 * time.Hour)
	return &Service{tracker: tk, startDate: start, date: start, jobTypes: types}, nil
}

func post(ctx context.Context, url url.URL) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, "POST", url.String(), nil)
	if reqErr != nil {
		return nil, 0, reqErr
	}
	resp, postErr := http.DefaultClient.Do(req)
	if postErr != nil {
		return nil, 0, postErr // Documentation says we can ignore body.
	}

	// Guaranteed to have a non-nil response and body.
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body) // Documentation recommends reading body.
	return b, resp.StatusCode, err
}

// NextJob is used by clients to fetch the next job from the service.
func NextJob(ctx context.Context, base url.URL) (tracker.Job, error) {
	jobURL := base
	jobURL.Path = "job"

	job := tracker.Job{}

	b, status, err := post(ctx, jobURL)
	if err != nil {
		return job, err
	}
	if status != http.StatusOK {
		return job, errors.New(http.StatusText(status))
	}

	err = json.Unmarshal(b, &job)
	return job, err
}
