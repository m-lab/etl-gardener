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

// Waits for bqjob to complete, handles backoff and job updates.
// Returns non-nil status if successful.
func waitAndCheck(ctx context.Context, tk *tracker.Tracker, bqJob bqiface.Job, j tracker.Job, s tracker.Status, label string) *bigquery.JobStatus {
	status, err := bqJob.Wait(ctx)
	if err != nil {
		switch typedErr := err.(type) {
		case *googleapi.Error:
			if typedErr.Code == http.StatusBadRequest &&
				strings.Contains(typedErr.Error(), "streaming buffer") {
				// Wait a while and try again.
				log.Println(typedErr)
				metrics.WarningCount.WithLabelValues(
					j.Experiment, j.Datatype,
					label+"WaitingForStreamingBuffer").Inc()
				s.UpdateDetail("waiting for empty streaming buffer")
				tk.UpdateJob(j, s)
				time.Sleep(2 * time.Minute)
				return nil
			}
		default:
			// We don't know the problem...
		}
		log.Println(label, err)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownError").Inc()
		// This will terminate this job.
		tk.SetJobError(j, err.Error())
		return nil
	}
	if status.Err() != nil {
		err := status.Err()
		log.Println(label, err)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownStatusError").Inc()

		// This will terminate this job.
		tk.SetJobError(j, err.Error())
		return nil
	}
	return status
}

// TODO figure out how to test this code?
func dedupFunc(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
	start := time.Now()
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	delay := time.Since(s.LastStateChangeTime()).Round(time.Minute)

	var bqJob bqiface.Job
	var msg string
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(j, os.Getenv("PROJECT"))
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
	status := waitAndCheck(ctx, tk, bqJob, j, s, "Dedup")

	if status == nil {
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
