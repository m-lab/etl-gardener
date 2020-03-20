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
	"os"
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

// NewJobService creates the default job service.
func NewJobService(tk *tracker.Tracker, bucket string, startDate time.Time) (*Service, error) {
	// TODO construct the jobs from storage bucket.
	// The service cycles through the jobSpecs.  Each spec is a job (bucket/exp/type) and a target GCS bucket or BQ table.
	// TODO get the destination bucket from a command line flag.
	targetBase := os.Getenv("TARGET_BASE")
	j1, _ := tracker.Job{Bucket: bucket, Experiment: "ndt", Datatype: "ndt5"}.Target(targetBase + "/ndt/ndt5")
	j2, _ := tracker.Job{Bucket: bucket, Experiment: "ndt", Datatype: "tcpinfo"}.Target(targetBase + "/ndt/tcpinfo")
	specs := []tracker.JobWithTarget{j1, j2}

	start := startDate.UTC().Truncate(24 * time.Hour)
	date := start
	index := 0
	if tk != nil {
		lastInit := tk.LastJob()
		log.Println("Last job was:", lastInit)
		date := lastInit.Date
		if lastInit == j1.Job {
			index = 1
		} else {
			date = date.AddDate(0, 0, 1)
		}
		if date.Before(start) {
			date = start
			index = 0
		}
	}
	return &Service{tracker: tk, startDate: start, date: date, nextIndex: index, jobSpecs: specs}, nil
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
func NextJob(ctx context.Context, base url.URL) (tracker.JobWithTarget, error) {
	jobURL := base
	jobURL.Path = "job"

	job := tracker.JobWithTarget{}

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
