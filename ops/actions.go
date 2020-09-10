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

// This returns a ConditionFunc that checks for completion of corresponding
// annotation job in the tracker.
func newCondFunc(tk *tracker.Tracker, detail string) ConditionFunc {
	return func(ctx context.Context, j tracker.Job) bool {
		if j.Datatype == "annotation" {
			return true
		}
		ann := j
		ann.Datatype = "annotation"
		status, err := tk.GetStatus(j)
		if err != nil {
			return true // Hack
		}
		return status.State() == tracker.Complete
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
		tracker.Loading)
	m.AddAction(tracker.Loading,
		nil,
		loadFunc,
		tracker.Deduplicating)
	m.AddAction(tracker.Deduplicating,
		nil,
		dedupFunc,
		tracker.Copying)
	m.AddAction(tracker.Copying,
		nil,
		copyFunc,
		tracker.Deleting)
	m.AddAction(tracker.Deleting,
		nil,
		deleteFunc,
		tracker.Joining)
	m.AddAction(tracker.Joining,
		newCondFunc(tk, "Join condition"),
		joinFunc,
		tracker.Complete)
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

// TODO - would be nice to persist this object, instead of creating it
// repeatedly.  If we end up with separate state machine per job, that
// would be a good place for the TableOps object.
func tableOps(ctx context.Context, j tracker.Job) (*bq.TableOps, error) {
	// TODO pass in the JobWithTarget, and get this info from Target.
	project := os.Getenv("PROJECT")
	loadSource := fmt.Sprintf("gs://etl-%s/%s/%s/%s",
		project,
		j.Experiment, j.Datatype, j.Date.Format("2006/01/02/*"))
	return bq.NewTableOps(ctx, j, project, loadSource)
}

func interpretStatus(op string, j tracker.Job, status *bigquery.JobStatus, delay time.Duration) string {
	var msg string
	stats := status.Statistics
	switch details := stats.Details.(type) {
	case *bigquery.QueryStatistics:
		opTime := stats.EndTime.Sub(stats.StartTime)
		metrics.QueryCostHistogram.WithLabelValues(j.Datatype, op).Observe(float64(details.SlotMillis) / 1000.0)
		msg = fmt.Sprintf("%s took %s (after %v waiting), %5.2f Slot Minutes, %d Rows affected, %d MB Processed, %d MB Billed",
			op,
			opTime.Round(100*time.Millisecond),
			delay,
			float64(details.SlotMillis)/60000, details.NumDMLAffectedRows,
			details.TotalBytesProcessed/1000000, details.TotalBytesBilled/1000000)
		log.Println(msg)
		log.Printf("%s %s: %+v\n", op, j, details)
	default:
		log.Printf("Could not convert to QueryStatistics: %+v\n", status.Statistics.Details)
		msg = "Could not convert Detail to QueryStatistics"
	}
	return msg
}

// TODO improve test coverage?
func dedupFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	delay := time.Since(stateChangeTime).Round(time.Minute)

	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := tableOps(ctx, j)
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err := qp.Dedup(ctx, false)
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
	msg := interpretStatus("Dedup", j, status, delay)
	return Success(j, msg)
}

func handleLoadError(label string, j tracker.Job, status *bigquery.JobStatus) *Outcome {
	err := status.Err()
	log.Println(label, err)
	msg := "unknown error"
	for _, e := range status.Errors {
		if strings.Contains(e.Message, "Please look into") {
			metrics.WarningCount.WithLabelValues(
				j.Experiment, j.Datatype,
				label+"-PleaseLookInto").Inc()
			continue
		}
		// When there is mismatch between row content and table schema,
		// the bigquery Load may return "No such field:" errors.
		if strings.Contains(e.Message, "No such field:") {
			log.Printf("--- Field missing in bigquery: %s: %s -- %s", label, e.Message, e.Location)
			metrics.WarningCount.WithLabelValues(
				j.Experiment, j.Datatype,
				label+"GCS load failed - missing field").Inc()
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
		if status != nil {
			outcome := handleWaitError(label, j, status)
			return status, outcome
		}
		return status, Failure(j, err, "unknown error")
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

	qp, err := tableOps(ctx, j)
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err := qp.LoadToTmp(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Load")
	if !outcome.IsDone() {
		return outcome
	}

	msg := "nil stats" // In case stats are nil.
	stats := status.Statistics
	if stats != nil {
		// TODO: Add a histogram metric
		opTime := stats.EndTime.Sub(stats.StartTime)
		details := stats.Details
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
			msg = "Load statistics unknown type"
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

	qp, err := tableOps(ctx, j)
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err := qp.CopyToRaw(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Copy")
	if !outcome.IsDone() {
		return outcome
	}

	msg := "nil stats"
	stats := status.Statistics
	if stats != nil {
		// TODO: Add a histogram metric
		opTime := stats.EndTime.Sub(stats.StartTime)
		msg = fmt.Sprintf("Copy took %s (after %s waiting), %d MB Processed",
			opTime.Round(100*time.Millisecond),
			delay,
			stats.TotalBytesProcessed/1000000)
	}
	log.Println(j, msg)
	return Success(j, msg)
}

// TODO improve test coverage?
func deleteFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := tableOps(ctx, j)
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

func joinFunc(ctx context.Context, j tracker.Job, stateChangeTime time.Time) *Outcome {
	if j.Datatype == "annotation" {
		// annotation should not be annotated.
		return Success(j, "Annotation does not require join")
	}
	// TODO pass in the JobWithTarget, and get the base from the target.
	delay := time.Since(stateChangeTime).Round(time.Minute)
	to, err := tableOps(ctx, j)
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}

	bqJob, err := to.Join(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Join")
	if !outcome.IsDone() {
		return outcome
	}

	// Join job was successful.  Handle the statistics, metrics, tracker update.
	msg := interpretStatus("Join", j, status, delay)
	return Success(j, msg)
}
