package ops

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/dedup"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/etl-gardener/tracker"
)

// PartitionedTable creates BQ Table for legacy source templated table
func isTest() bool {
	return flag.Lookup("test.v") != nil
}

func newStateFunc(state tracker.State, detail string) ActionFunc {
	return func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
		log.Println(j, state)
		metrics.StateDate.WithLabelValues(j.Experiment, j.Datatype, string(state)).Set(float64(j.Date.Unix()))
		err := tk.SetStatus(j, state, detail)
		if err != nil {
			log.Println(err)
		}
	}
}

// NewStandardMonitor creates the standard monitor that handles several state transitions.
// TODO Finishing action is incomplete.
func NewStandardMonitor(ctx context.Context, config cloud.BQConfig, tk *tracker.Tracker) (*Monitor, error) {
	m, err := NewMonitor(ctx, config, tk)
	if err != nil {
		return nil, err
	}
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc(tracker.Deduplicating, "-"),
		"Changing to Deduplicating")
	// Hack to handle old jobs from previous gardener implementations
	m.AddAction(tracker.Stabilizing,
		nil,
		newStateFunc(tracker.Deduplicating, "-"),
		"Changing to Deduplicating")
	m.AddAction(tracker.Deduplicating,
		nil,
		dedupFunc,
		"Deduplicating")
	return m, nil
}

func dedupFunc(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
	start := time.Now()
	if j.Datatype == "tcpinfo" {
		// TODO fix this HACK
		qp := dedup.TCPInfoQuery(j, os.Getenv("TARGET_BASE"))
		bqJob, err := qp.Dedup(ctx)
		if err != nil {
			switch typedErr := err.(type) {
			case *googleapi.Error:
				if typedErr.Code == http.StatusBadRequest &&
					strings.Contains(typedErr.Error(), "streaming buffer") {
					// Wait a while and try again.
					s.UpdateDetail = "Dedup waiting for empty streaming buffer."
					tk.UpdateJob(j, s)
					time.Sleep(5 * time.Minute)
					return // Try again later.
				}
			default:
				if err == state.ErrBQRateLimitExceeded {
					s.UpdateDetail = "Dedup waiting because of BQ Rate Limit Exceeded."
					tk.UpdateJob(j, s)
					time.Sleep(5 * time.Minute)
					return // Try again later.
				}
			}

			log.Println(err)
			tk.SetJobError(j, err.Error())
			return
		}
		status, err := bqJob.Wait(ctx)
		if err != nil {
			log.Println(err)
			err = tk.SetJobError(j, "dedup failed"+err.Error())
			return
		}
		log.Printf("%+v\n", status.Statistics.Details)
	} else if j.Datatype == "ndt5" {
		tk.SetJobError(j, "dedup not implemented for ndt5")
		return
	} else {
		tk.SetJobError(j, "unknown datatype")
		return
	}
	s.State = tracker.Complete
	metrics.StateDate.WithLabelValues(j.Experiment, j.Datatype, string(tracker.Complete)).Set(float64(j.Date.Unix()))
	err := tk.SetStatus(j, tracker.Complete, "dedup took "+time.Since(start).Round(100*time.Millisecond).String())
	if err != nil {
		log.Println(err)
	}
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - why don't we just use job.Wait()?  Just because of terminate?
// TODO - develop a BQJob interface for wrapping bigquery.Job, and allowing fakes.
// TODO - move this to go/dataset, since it is bigquery specific and general purpose.
func waitForJob(ctx context.Context, job tracker.Job, bqJob bqiface.Job, maxBackoff time.Duration) error {
	log.Println("Wait for job:", bqJob.ID())
	backoff := 10 * time.Second // Some jobs finish much quicker, but we don't really care that much.
	previous := backoff
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			status, err := bqJob.Wait(ctx)
			if err != nil {
				log.Println(job.String(), bqJob.ID(), err)
				return err
			} else if status.Err() != nil {
				// NOTE we are getting rate limit exceeded errors here.
				log.Println(job, bqJob.ID(), status.Err())
				if strings.Contains(status.Err().Error(), "rateLimitExceeded") {
					return state.ErrBQRateLimitExceeded
				}
				if strings.Contains(status.Err().Error(), "Not found: Table") {
					return state.ErrTableNotFound
				}
				if strings.Contains(status.Err().Error(), "rows belong to different partitions") {
					return state.ErrRowsFromOtherPartition
				}
			} else if status.Done() {
				// TODO add metrics for dedup statistics.
				log.Printf("DONE: %s (%s) TotalBytes: %d\n", job, bqJob.ID(), status.Statistics.TotalBytesProcessed)
				return nil
			}
			if backoff+previous < maxBackoff {
				tmp := previous
				previous = backoff
				backoff = backoff + tmp
			} else {
				backoff = maxBackoff
			}
		}
		time.Sleep(backoff)
	}
}
