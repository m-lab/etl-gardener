package ops

import (
	"context"
	"errors"
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

func newStateFunc(detail string) ActionFunc {
	return func(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
		return Success(j, detail)
	}
}

// NewStandardMonitor creates the standard monitor that handles several state transitions.
func NewStandardMonitor(ctx context.Context, config cloud.BQConfig, tk *tracker.Tracker) (*Monitor, error) {
	m, err := NewMonitor(ctx, config, tk)
	if err != nil {
		return nil, err
	}
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc("-"),
		tracker.Loading,
		"Changing to Loading")
	// Hack to handle old jobs from previous gardener implementations
	m.AddAction(tracker.Loading,
		nil,
		loadFunc,
		tracker.Deduplicating,
		"Loading")
	m.AddAction(tracker.Deduplicating,
		nil,
		dedupFunc,
		tracker.Copying,
		"Deduplicating")
	m.AddAction(tracker.Copying,
		nil,
		copyFunc,
		tracker.Deleting,
		"Copying")
	m.AddAction(tracker.Deleting,
		nil,
		deleteFunc,
		tracker.Complete,
		"Deleting")
	return m, nil
}

// Waits for bqjob to complete, handles backoff and job updates.
// Returns non-nil status if successful.
func waitAndCheck(ctx context.Context, bqJob bqiface.Job, j tracker.Job, label string) (*bigquery.JobStatus, *Outcome) {
	status, err := bqJob.Wait(ctx)
	if err != nil {
		switch typedErr := err.(type) {
		case *googleapi.Error:
			if typedErr.Code == http.StatusBadRequest &&
				strings.Contains(typedErr.Error(), "streaming buffer") {
				log.Println(typedErr)
				metrics.WarningCount.WithLabelValues(
					j.Experiment, j.Datatype,
					label+"WaitingForStreamingBuffer").Inc()

				// Leave in current state, Wait a while and try again.
				return nil, Retry(j, err, "waiting for empty streaming buffer")
			}
			log.Println(typedErr, typedErr.Code)
		default:
			// We don't know the problem...
		}
		// Not googleapi.Error, OR not streaming buffer problem.
		log.Println(j, label, err)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownError").Inc()
		// This will terminate this job.
		return status, Failure(j, err, "unknown error")
	}
	if status.Err() != nil {
		err := status.Err()
		log.Println(j, label, err)
		for i := range status.Errors {
			log.Println("---", j, label, status.Errors[i])
		}
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownStatusError").Inc()

		// This will terminate this job.
		return status, Failure(j, err, "unknown error")
	}
	return status, Success(j, "-")
}

// TODO improve test coverage?
func dedupFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	delay := time.Since(stateChangeTime).Round(time.Minute)

	var bqJob bqiface.Job
	var msg string
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(ctx, j, os.Getenv("PROJECT"), "")
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.Dedup(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Dedup")
	if !outcome.IsDone() {
		return outcome
	}
	if status == nil {
		// Nil status means the job failed.
		return Failure(j, errors.New("nil status"), "-")
	}

	// Dedup job was successful.  Handle the statistics, metrics, tracker update.
	stats := status.Statistics
	switch details := stats.Details.(type) {
	case *bigquery.QueryStatistics:
		opTime := stats.EndTime.Sub(stats.StartTime)
		metrics.QueryCostHistogram.WithLabelValues(j.Datatype, "dedup").Observe(float64(details.SlotMillis) / 1000.0)
		msg = fmt.Sprintf("Dedup took %s (after %v waiting), %5.2f Slot Minutes, %d Rows affected, %d MB Processed, %d MB Billed",
			opTime.Round(100*time.Millisecond),
			delay,
			float64(details.SlotMillis)/60000, details.NumDMLAffectedRows,
			details.TotalBytesProcessed/1000000, details.TotalBytesBilled/1000000)
		log.Println(msg)
		log.Printf("Dedup %s: %+v\n", j, details)
	default:
		log.Printf("Could not convert to QueryStatistics: %+v\n", status.Statistics.Details)
		msg = "Could not convert Detail to QueryStatistics"
	}

	return Success(j, msg)
}

func handleLoadError(label string, j tracker.Job, status *bigquery.JobStatus) *Outcome {
	err := status.Err()
	log.Println(label, err)
	msg := "unknown error"
	for _, e := range status.Errors {
		if strings.Contains(e.Message, "Please look into") {
			continue
		}
		if strings.Contains(e.Message, "No such field:") {
			log.Printf("--- %s: %s -- %s", label, e.Message, e.Location)
			metrics.WarningCount.WithLabelValues(
				j.Experiment, j.Datatype,
				label+"GCS load failed").Inc()
			msg = e.Message
			continue
		}
		log.Println("---", label, e)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownStatusError").Inc()
	}

	// This will terminate this job.
	return Failure(j, err, msg)
}

func handleWaitError(label string, j tracker.Job, status *bigquery.JobStatus) *Outcome {
	err := status.Err()
	log.Println(label, err)
	for i := range status.Errors {
		log.Println("---", label, status.Errors[i])
	}
	metrics.WarningCount.WithLabelValues(
		j.Experiment, j.Datatype,
		label+"UnknownStatusError").Inc()

	// This will terminate this job.
	return Failure(j, err, "unknown error")
}

// Returns non-nil Outcome even if successful.
func waitForLoad(ctx context.Context, bqJob bqiface.Job, j tracker.Job, label string) (*bigquery.JobStatus, *Outcome) {
	status, err := bqJob.Wait(ctx)
	if err != nil {
		outcome := handleWaitError(label, j, status)
		return status, outcome
	}
	if status.Err() != nil {
		outcome := handleLoadError(label, j, status)
		return status, outcome
	}
	return status, Success(j, "-")
}

// TODO improve test coverage?
func loadFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	delay := time.Since(stateChangeTime).Round(time.Minute)

	var bqJob bqiface.Job
	// TODO pass in the JobWithTarget, and get this info from Target.
	project := os.Getenv("PROJECT")
	loadSource := fmt.Sprintf("gs://json-%s/%s/%s/%s",
		project,
		j.Experiment, j.Datatype, j.Date.Format("2006/01/02/*"))
	qp, err := bq.NewQuerier(ctx, j, project, loadSource)
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.LoadToTmp(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Load")
	if !outcome.IsDone() {
		return outcome
	}

	var msg string
	stats := status.Statistics
	if stats != nil {
		opTime := stats.EndTime.Sub(stats.StartTime)
		details := status.Statistics.Details
		switch td := details.(type) {
		case *bigquery.LoadStatistics:
			metrics.FilesPerDateHistogram.WithLabelValues(
				j.Experiment+"-json", j.Datatype, j.Date.Format("2006-01")).Observe(float64(td.InputFiles))
			metrics.BytesPerDateHistogram.WithLabelValues(
				j.Experiment+"-json", j.Datatype, j.Date.Format("2006-01")).Observe(float64(td.InputFileBytes))
			msg = fmt.Sprintf("Load took %s (after %s waiting), %d rows with %d bytes, from %d files with %d bytes",
				opTime.Round(100*time.Millisecond),
				delay,
				td.OutputRows, td.OutputBytes,
				td.InputFiles, td.InputFileBytes)
		default:
			msg = "Load statistics unavailable"
		}
	}
	log.Println(j, msg)
	return Success(j, msg)
}

// TODO improve test coverage?
func copyFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	delay := time.Since(stateChangeTime).Round(time.Minute)

	var bqJob bqiface.Job
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(ctx, j, os.Getenv("PROJECT"), "")
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.CopyToRaw(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Copy")
	if !outcome.IsDone() {
		return outcome
	}

	var msg string
	stats := status.Statistics
	if stats != nil {
		opTime := stats.EndTime.Sub(stats.StartTime)
		msg = fmt.Sprintf("Copy took %s (after %s waiting), %d MB Processed",
			opTime.Round(100*time.Millisecond),
			delay,
			stats.TotalBytesProcessed/1000000)
		log.Println(msg)
	}
	return Success(j, msg)
}

// TODO improve test coverage?
func deleteFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(ctx, j, os.Getenv("PROJECT"), "")
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	err = qp.DeleteTmp(ctx)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}

	// TODO - add elapsed time to message.
	return Success(j, "Successfully deleted partition")
}
