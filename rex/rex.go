// Package rex ...
package rex

// TODO - work out how to manage queues and new tasks.
// Some simplificaitons:
// 1. available queues should come from config, not state.
// 2. Tasks contain all info about queues in use.
// 3. Don't need to list the tasks in the persistent state, as they are available from DS

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/dispatch"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

// Environment provides "global" variables.
type environment struct {
	TestMode bool
}

// env provides environment vars.
var env environment

func init() {
	// HACK This allows some modified behavior when running unit tests.
	if flag.Lookup("test.v") != nil {
		env.TestMode = true
	}
}

// ReprocessingExecutor handles all reprocessing steps.
type ReprocessingExecutor struct {
	cloud.BQConfig
	BucketOpts []option.ClientOption
}

// AdvanceState advances task to the next state.
func (rex *ReprocessingExecutor) AdvanceState(t *state.Task) {
	switch t.State {
	case state.Initializing:
		t.Update(state.Queuing)
	case state.Queuing:
		t.Update(state.Processing)
	case state.Processing:
		t.Queue = "" // No longer need to keep the queue.
		t.Update(state.Stabilizing)
	case state.Stabilizing:
		t.Update(state.Deduplicating)
	case state.Deduplicating:
		t.Update(state.Finishing)
	case state.Finishing:
		t.JobID = ""
		t.Update(state.Done)
	case state.Done:
	}
}

// GetDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetDS() (bqext.Dataset, error) {
	return bqext.NewDataset(rex.BQProject, rex.BQDataset, rex.Options...)
}

// ErrNoSaver is returned when saver has not been set.
var ErrNoSaver = errors.New("Task.saver is nil")

// DoAction executes the currently specified action and update Err/ErrInfo
// It may terminate prematurely if terminate is closed prior to completion.
// TODO - consider returning any error to caller.
func (rex *ReprocessingExecutor) DoAction(t *state.Task, terminate <-chan struct{}) {
	// Note that only the state in state.Task is maintained between actions.
	switch t.State {
	case state.Initializing:
		// TODO - should check that _suffix table doesn't already exist, or delete it
		// if it does?  Also check if it has a streaming buffer?
		// Nothing to do.
	case state.Queuing:
		rex.queue(t, terminate)
	case state.Processing: // TODO should this be Parsing?
		rex.waitForParsing(t, terminate)
	case state.Stabilizing:
		// Wait for the streaming buffer to be nil.
		ds, err := rex.GetDS()
		if err != nil {
			t.SetError(err, "GetDS")
			return
		}
		s, _, err := t.SourceAndDest(&ds)
		if err != nil {
			t.SetError(err, "SourceAndDest")
			return
		}
		err = dispatch.WaitForStableTable(s)
		if err != nil {
			t.SetError(err, "WaitForStableTable")
			return
		}
	case state.Deduplicating:
		rex.dedup(t, terminate)
	case state.Finishing:
		rex.finish(t, terminate)
	case state.Done:
		t.Delete()
	}
}

// TODO should these take Task instead of *Task?
// TODO - this replaces the wait part of dispatch.waitAndDedup.  Remove obsolete code
// when this is deployed.
func (rex *ReprocessingExecutor) waitForParsing(t *state.Task, terminate <-chan struct{}) {
	// Wait for the queue to drain.
	// Don't want to accept a date until we can actually queue it.
	qh, err := tq.NewQueueHandler(rex.Config, t.Queue)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewQueueHandler")
		t.SetError(err, "NewQueueHandler")
		return
	}
	log.Println("Wait for empty queue ", qh.Queue)
	for err := qh.IsEmpty(); err != nil; err = qh.IsEmpty() {
		select {
		case <-terminate:
			t.SetError(err, "Terminating")
			return
		default:
		}
		if err == tq.ErrMoreTasks {
			// Wait 5-15 seconds before checking again.
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		} else if err != nil {
			if err == io.EOF && env.TestMode {
				// Expected when using test client.
				return
			}
			// We don't expect errors here, so try logging, and a large backoff
			// in case there is some bad network condition, service failure,
			// or perhaps the queue_pusher is down.
			log.Println(err)
			metrics.WarningCount.WithLabelValues("IsEmptyError").Inc()
			// TODO update metric
			time.Sleep(time.Duration(60+rand.Intn(120)) * time.Second)
		}
	}
}

// TODO - this replaces ChannelQueueHandler.processOneRequest.  Should delete that
// code after transition.
func (rex *ReprocessingExecutor) queue(t *state.Task, terminate <-chan struct{}) {
	// Submit all files from the bucket that match the prefix.
	// Where do we get the bucket?
	//func (qh *ChannelQueueHandler) handleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	qh, err := tq.NewQueueHandler(rex.Config, t.Queue)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewQueueHandler")
		t.SetError(err, "NewQueueHandler")
		return
	}
	parts, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		metrics.FailCount.WithLabelValues("BadPrefix").Inc()
		t.SetError(err, "BadPrefix")
		return
	}
	bucketName := parts[1]
	bucket, err := tq.GetBucket(rex.BucketOpts, rex.Project, bucketName, false)
	if err != nil {
		if err == io.EOF && env.TestMode {
			log.Println("Using fake client, so can't get real bucket")
		}
		log.Println(err)
		metrics.FailCount.WithLabelValues("BucketError").Inc()
		t.SetError(err, "BucketError")
		return
	}
	// TODO maybe check terminate while queuing?
	n, err := qh.PostDay(bucket, bucketName, parts[2]+"/"+parts[3]+"/")
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("PostDayError").Inc()
		t.SetError(err, "PostDayError")
		return
	}
	log.Println("Added ", n, t.Name, " tasks to ", qh.Queue)
}

// TODO - this replaces part of the code in dispatch.waitAndDedup.  Should migrate
// Dedup() here, and delete obsolete code.
func (rex *ReprocessingExecutor) dedup(t *state.Task, terminate <-chan struct{}) {
	// Launch the dedup request, and save the JobID
	ds, err := rex.GetDS()
	if err != nil {
		metrics.FailCount.WithLabelValues("NewDataset")
		t.SetError(err, "GetDS")
		return
	}
	src, dest, err := t.SourceAndDest(&ds)
	if err != nil {
		metrics.FailCount.WithLabelValues("SourceAndDest")
		t.SetError(err, "SourceAndDest")
		return
	}

	log.Println("Dedupping", src.FullyQualifiedName())
	// TODO move Dedup??
	job, err := dispatch.Dedup(&ds, src.TableID, dest)
	if err != nil {
		if err == io.EOF {
			if env.TestMode {
				t.JobID = "fakeJobID"
				return
			}
		} else {
			log.Println(err, src.FullyQualifiedName())
			metrics.FailCount.WithLabelValues("DedupFailed")
			t.SetError(err, "DedupFailed")
			return
		}
	}
	t.JobID = job.ID()
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - develop a BQJob interface for wrapping bigquery.Job, and allowing fakes.
// TODO - move this to go/bqext, since it is bigquery specific and general purpose.
func waitForJob(ctx context.Context, job *bigquery.Job, maxBackoff time.Duration, terminate <-chan struct{}) error {
	backoff := 10 * time.Millisecond
	previous := backoff
	for {
		select {
		case <-terminate:
			return state.ErrTaskSuspended
		default:
		}
		status, err := job.Status(ctx)
		if err != nil {
			continue
		}
		if status.Err() != nil {
			continue
		}
		if status.Done() {
			break
		}
		if backoff+previous < maxBackoff {
			tmp := previous
			previous = backoff
			backoff = backoff + tmp
		} else {
			backoff = maxBackoff
		}

		time.Sleep(backoff)
	}
	return nil
}

// TODO - this replaces the latter part of dispatch.waitAndDedup().  Remove obsolete code
// after deployment.
func (rex *ReprocessingExecutor) finish(t *state.Task, terminate <-chan struct{}) {
	// TODO use a simple client instead of creating dataset?
	ds, err := bqext.NewDataset(rex.Project, rex.BQDataset, rex.Options...)
	if err != nil {
		metrics.FailCount.WithLabelValues("NewDataset")
		t.SetError(err, "NewDataset")
		return
	}
	src, _, err := t.SourceAndDest(&ds)
	if err != nil {
		metrics.FailCount.WithLabelValues("SourceAndDest")
		t.SetError(err, "SourceAndDest")
		return
	}
	job, err := ds.BqClient.JobFromID(context.Background(), t.JobID)
	if err != nil {
		metrics.FailCount.WithLabelValues("JobFromID")
		t.SetError(err, "JobFromID")
		return
	}
	// TODO - should loop, and check terminate channel
	err = waitForJob(context.Background(), job, 10*time.Second, terminate)
	status, err := job.Wait(context.Background())
	if err != nil {
		if err != state.ErrTaskSuspended {
			log.Println(status.Err(), src.FullyQualifiedName())
			metrics.FailCount.WithLabelValues("DedupJobWait")
			t.SetError(err, "DedupJobWait")
		}
		return
	}

	// Wait for JobID to complete, then delete the template table.
	log.Println("Completed deduplication, deleting", src.FullyQualifiedName())
	// If deduplication was successful, we should delete the source table.
	ctx, cf := context.WithTimeout(context.Background(), time.Minute)
	defer cf()
	err = src.Delete(ctx)
	if err != nil {
		metrics.FailCount.WithLabelValues("TableDeleteErr")
		log.Println(err)
	}
	if ctx.Err() != nil {
		if ctx.Err() != context.DeadlineExceeded {
			metrics.FailCount.WithLabelValues("TableDeleteTimeout")
			t.SetError(err, "TableDeleteTimeout")
			log.Println(ctx.Err())
			return
		}
	}

	// TODO - Copy to base_tables.
}
