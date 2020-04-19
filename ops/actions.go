package ops

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/etl-gardener/tracker"
)

// PartitionedTable creates BQ Table for legacy source templated table
func isTest() bool {
	return flag.Lookup("test.v") != nil
}

func newStateFunc(state tracker.State, detail string) ActionFunc {
	return func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, unused tracker.Status) {
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

// TODO figure out how to test this code?
func dedupFunc(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
	start := time.Now()
	// This is the delay since entering the dedup state.
	delay := time.Since(s.LastStateChangeTime()).Round(time.Minute)

	var bqJob bqiface.Job
	var msg string
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQueryParams(j, os.Getenv("PROJECT"))
	if err != nil {
		log.Println(err)
		// This terminates this job.
		tk.SetJobError(j, err.Error())
		return
	}
	bqJob, err = qp.Run(ctx, "dedup", false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return
	}
	status, err := bqJob.Wait(ctx)
	if err != nil {
		switch typedErr := err.(type) {
		case *googleapi.Error:
			if typedErr.Code == http.StatusBadRequest &&
				strings.Contains(typedErr.Error(), "streaming buffer") {
				// Wait a while and try again.
				s.UpdateDetail("Dedup waiting for empty streaming buffer.")
				tk.UpdateJob(j, s)
			}
		default:
			// We don't know the problem...
			log.Println(err)
		}
		time.Sleep(2 * time.Minute)
		return // Try again later.
	}
	if status.Err() != nil {
		err := status.Err()
		switch typedErr := err.(type) {
		case *googleapi.Error:
			log.Println(err)
			if typedErr.Code == http.StatusBadRequest &&
				strings.Contains(typedErr.Error(), "streaming buffer") {
				// Wait a while and try again.
				// Since there is no job update, the tracker may eventually kill
				// this job if it doesn't succeed within the stale job time limit.
				s.UpdateDetail("Dedup waiting for empty streaming buffer.")
				tk.UpdateJob(j, s)
				time.Sleep(2 * time.Minute)
				return // Try again later.
			}
		default:
			log.Println(err)
			if err == state.ErrBQRateLimitExceeded {
				// If BQ is rate limited, this basically results in jobs queuing up
				// and executing later.
				// Since there is no job update, the tracker may eventually kill
				// this job if it doesn't succeed within the stale job time limit.
				// TODO should this use exponential backoff?
				s.UpdateDetail("Dedup waiting because of BQ Rate Limit Exceeded.")
				tk.UpdateJob(j, s)
				time.Sleep(5 * time.Minute)
				return // Try again later.
			}
		}

		// This will terminate this job.
		tk.SetJobError(j, err.Error())
		return
	}

	// Dedup job was successful.  Handle the statistics, metrics, tracker update.
	switch details := status.Statistics.Details.(type) {
	case *bigquery.QueryStatistics:
		metrics.QueryCostHistogram.WithLabelValues(j.Datatype, "dedup").Observe(float64(details.SlotMillis) / 1000.0)
		msg = fmt.Sprintf("Dedup took %s (after %s waiting), %5.2f Slot Minutes, %d Rows affected, %d MB Processed, %d MB Billed",
			time.Since(start).Round(100*time.Millisecond).String(),
			delay,
			float64(details.SlotMillis)/60000, details.NumDMLAffectedRows,
			details.TotalBytesProcessed/1000000, details.TotalBytesBilled/1000000)
		log.Println(msg)
		log.Printf("Dedup %s: %+v\n", j, details)
	default:
		log.Printf("Could not convert to QueryStatistics: %+v\n", status.Statistics.Details)
		msg = "Could not convert Detail to QueryStatistics"
	}
	metrics.StateDate.WithLabelValues(j.Experiment, j.Datatype, string(tracker.Complete)).Set(float64(j.Date.Unix()))
	err = tk.SetStatus(j, tracker.Complete, msg)
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
