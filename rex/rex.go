// Package rex provides ReprocessingExecutor, which implements the state.Executor interface.
// It provides all the implementation detail for reprocessing data through the etl pipeline.
package rex

// Design Notes:
// 1. Tasks contain all info about queues in use.
// 2. Tasks in flight are not cached locally, as they are available from Datastore.

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/option"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
)

func isTest() bool {
	return flag.Lookup("test.v") != nil
}

// ReprocessingExecutor handles all reprocessing steps.
type ReprocessingExecutor struct {
	cloud.BQConfig
	StorageClient stiface.Client
}

// NewReprocessingExecutor creates a new exec.
// NOTE:  The context is used to create a persistent storage Client!
func NewReprocessingExecutor(ctx context.Context, config cloud.BQConfig, bucketOpts ...option.ClientOption) (*ReprocessingExecutor, error) {
	storageClient, err := storage.NewClient(ctx, bucketOpts...)
	if err != nil {
		return nil, err
	}
	return &ReprocessingExecutor{config, stiface.AdaptClient(storageClient)}, nil
}

// GetBatchDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetBatchDS(ctx context.Context) (dataset.Dataset, error) {
	return dataset.NewDataset(ctx, rex.BQProject, rex.BQBatchDataset, rex.Options...)
}

// GetFinalDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetFinalDS(ctx context.Context) (dataset.Dataset, error) {
	// TODO - dataset should use the provided context.
	return dataset.NewDataset(ctx, rex.BQProject, rex.BQFinalDataset, rex.Options...)
}

var (
	// ErrNoSaver is returned when saver has not been set.
	ErrNoSaver = errors.New("Task.saver is nil")
	// ErrDatasetNamesMatch is returned when a sanity check would compare a dataset to itself.
	ErrDatasetNamesMatch = errors.New("Dataset names should not be the same")
)

// Next executes the currently specified action, updates Err/ErrInfo, and advances
// the state.
// It may terminate prematurely if terminate is closed prior to completion.
// TODO - consider returning any error to caller.
func (rex *ReprocessingExecutor) Next(ctx context.Context, t *state.Task, terminate <-chan struct{}) error {
	// Note that only the state in state.Task is maintained between actions.
	switch t.State {
	case state.Initializing:
		// TODO - should check that _suffix table doesn't already exist, or delete it
		// if it does?  Also check if it has a streaming buffer?
		// Nothing to do.
		t.Update(ctx, state.Queuing)
		metrics.StartedCount.WithLabelValues("sidestream").Inc()

	case state.Queuing:
		// TODO - handle zero task case.
		fileCount, byteCount, err := rex.queue(ctx, t)
		// Update the metrics, even if there is an error, since the files were submitted to the queue already.
		metrics.FilesPerDateHistogram.WithLabelValues(t.Experiment, "", strconv.Itoa(t.Date.Year())).Observe(float64(fileCount))
		metrics.BytesPerDateHistogram.WithLabelValues(t.Experiment, "", strconv.Itoa(t.Date.Year())).Observe(float64(byteCount))
		if err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.queue")
			return err
		}
		if fileCount < 1 {
			// If there were no tasks posted, then there is nothing more to do.
			t.Queue = ""
			t.Update(ctx, state.Done)
		} else {
			// TODO - should we also add metrics when there are errors?
			t.Update(ctx, state.Processing)
		}

	case state.Processing: // TODO should this be Parsing?
		log.Println("Processing:", t)
		if err := rex.waitForParsing(ctx, t, terminate); err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.waitForParsing")
			return err
		}
		t.Queue = "" // No longer need to keep the queue.
		t.Update(ctx, state.Stabilizing)

	case state.Stabilizing:
		log.Println("Stabilizing:", t)
		// Wait for the streaming buffer to be nil.
		ds, err := rex.GetBatchDS(ctx)
		if err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.GetBatchDS")
			return err
		}
		s, _, err := t.SourceAndDest(&ds)
		if err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "task.SourceAndDest")
			return err
		}
		err = bq.WaitForStableTable(ctx, s)
		if err != nil {
			// When testing, we expect to get ErrTableNotFound here.
			if err != state.ErrTableNotFound || !isTest() {
				t.SetError(ctx, err, "bq.WaitForStableTable")
				return err
			}
		}

		// Remove this when Gardener is working smoothly.
		// Log the stabilizing duration.  Some SanityChecks are failing, and they may
		// be related to premature exit from Stabiliting phase.
		duration := time.Since(t.UpdateTime)
		log.Println("Stabilizing", t.Name, "took", duration)

		t.Update(ctx, state.Deduplicating)

	case state.Deduplicating:
		log.Println("Deduplicating:", t)
		if err := rex.dedup(ctx, t); err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.dedup")
			return err
		}
		t.Update(ctx, state.Finishing)

	case state.Finishing:
		log.Println("Finishing:", t)
		if err := rex.finish(ctx, t, terminate); err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.finish")
			return err
		}
		t.JobID = ""
		t.Update(ctx, state.Done)

	case state.Done:
		log.Println("Unexpected call to Next when Done")

	case state.Invalid:
		log.Println("Should not call Next on Invalid state!")
		err := errors.New("called Next on invalid state")
		// SetError also pushes to datastore, like Update(ctx, )
		t.SetError(ctx, err, "Invalid state")
		return err

	default:
		log.Println("Unknown state")
		err := errors.New("Unknown state")
		// SetError also pushes to datastore, like Update(ctx, )
		t.SetError(ctx, err, "Unknown state")
		return err
	}

	return nil
}

// TODO should these take Task instead of *Task?
func (rex *ReprocessingExecutor) waitForParsing(ctx context.Context, t *state.Task, terminate <-chan struct{}) error {
	// Wait for the queue to drain.
	// Don't want to accept a date until we can actually queue it.
	qh, err := tq.NewQueueHandler(rex.Config, t.Queue)
	if err != nil {
		t.SetError(ctx, err, "NewQueueHandler")
		return err
	}
	log.Println("Wait for empty queue ", qh.Queue)
	for err := qh.WaitForEmptyQueue(terminate); err != nil; err = qh.WaitForEmptyQueue(terminate) {
		if err == tq.ErrTerminated {
			break
		}
		if err == io.EOF && isTest() {
			// Expected when using test client.
			log.Println("TestMode")
			return nil
		}
		// We don't expect errors here, so try logging, and a large backoff
		// in case there is some bad network condition, service failure,
		// or perhaps the queue_pusher is down.
		log.Println(err)
		metrics.WarningCount.WithLabelValues(t.Experiment, "", "IsEmptyError").Inc()
		// TODO update metric
		time.Sleep(time.Duration(60+rand.Intn(120)) * time.Second)
	}
	return nil
}

func (rex *ReprocessingExecutor) queue(ctx context.Context, t *state.Task) (int, int64, error) {
	// Submit all files from the bucket that match the prefix.
	// Where do we get the bucket?
	//func (qh *ChannelQueueHandler) handleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	qh, err := tq.NewQueueHandler(rex.Config, t.Queue)
	if err != nil {
		t.SetError(ctx, err, "NewQueueHandler")
		return 0, 0, err
	}
	prefix, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		t.SetError(ctx, err, "BadPrefix")
		return 0, 0, err
	}

	// Use a real storage bucket.
	// TODO - add a persistent storageClient to the rex object?
	// TODO - try cancelling the context instead?
	bucket, err := tq.GetBucket(ctx, rex.StorageClient, rex.Project, prefix.Bucket, false)
	if err != nil {
		if err == io.EOF && isTest() {
			log.Println("Using fake client, ignoring EOF error")
			return 0, 0, nil
		}
		log.Println(err)
		t.SetError(ctx, err, "BucketError")
		return 0, 0, err
	}

	// NOTE: This does not check the terminate channel, so once started, it will
	// complete the queuing.
	fileCount, byteCount, err := qh.PostDay(ctx, bucket, prefix.Bucket, prefix.Path())
	if err != nil {
		log.Println(err)
		t.SetError(ctx, err, "PostDayError")
		return fileCount, byteCount, nil
	}
	log.Println("Added ", byteCount, "bytes in", fileCount, t.Name, " tasks to ", qh.Queue)
	return fileCount, byteCount, nil
}

func (rex *ReprocessingExecutor) dedup(ctx context.Context, t *state.Task) error {
	// Launch the dedup request, and save the JobID
	ds, err := rex.GetBatchDS(ctx)
	if err != nil {
		t.SetError(ctx, err, "GetBatchDS")
		return err
	}
	src, dest, err := t.SourceAndDest(&ds)
	if err != nil {
		t.SetError(ctx, err, "SourceAndDest")
		return err
	}

	log.Println("Dedupping", src.FullyQualifiedName())
	// TODO move Dedup??
	// TODO - these sometimes fail with rateLimitExceeded.  Need to increase quota,
	// or implement a backoff.
	job, err := bq.Dedup(ctx, &ds, src.TableID(), dest)
	if err != nil {
		if err == io.EOF {
			if isTest() {
				t.JobID = "fakeJobID"
				return nil
			}
		} else {
			log.Println(err, src.FullyQualifiedName())
			t.SetError(ctx, err, "DedupFailed")
			return err
		}
	}
	t.JobID = job.ID()
	return nil
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - why don't we just use job.Wait()?  Just because of terminate?
// TODO - develop a BQJob interface for wrapping bigquery.Job, and allowing fakes.
// TODO - move this to go/dataset, since it is bigquery specific and general purpose.
func waitForJob(ctx context.Context, job bqiface.Job, maxBackoff time.Duration, terminate <-chan struct{}) error {
	backoff := 10 * time.Second // Some jobs finish much quicker, but we don't really care that much.
	previous := backoff
	for {
		select {
		case <-terminate:
			return state.ErrTaskSuspended
		default:
		}
		status, err := job.Status(ctx)
		if err != nil {
			log.Println(err)
		} else if status.Err() != nil {
			// NOTE we are getting rate limit exceeded errors here.
			log.Println(job.ID(), status.Err())
			if strings.Contains(status.Err().Error(), "rateLimitExceeded") {
				return state.ErrBQRateLimitExceeded
			}
			if strings.Contains(status.Err().Error(), "Not found: Table") {
				return state.ErrTableNotFound
			}
			if strings.Contains(status.Err().Error(), "rows belong to different partitions") {
				return state.ErrRowsFromOtherPartition
			}
			if backoff == maxBackoff {
				log.Println("reached max backoff")
				// return status.Err()
			}
		} else if status.Done() {
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

func (rex *ReprocessingExecutor) finish(ctx context.Context, t *state.Task, terminate <-chan struct{}) error {
	// TODO use a simple client instead of creating dataset?
	srcDs, err := rex.GetBatchDS(ctx)
	if err != nil {
		t.SetError(ctx, err, "GetBatchDS")
		return err
	}
	src, copy, err := t.SourceAndDest(&srcDs)
	if err != nil {
		t.SetError(ctx, err, "SourceAndDest")
		return err
	}
	job, err := srcDs.BqClient.JobFromID(ctx, t.JobID)
	if err != nil {
		t.SetError(ctx, err, "JobFromID")
		return err
	}
	// TODO - should loop, and check terminate channel
	err = waitForJob(ctx, job, 300*time.Second, terminate)
	if err != nil {
		log.Println(err, src.FullyQualifiedName())
		t.SetError(ctx, err, "waitForJob")
		return err
	}

	// This just gets the status.
	// TODO - should this context have a deadline?
	status, err := job.Wait(ctx)
	if err != nil {
		if err != state.ErrTaskSuspended {
			log.Println(err, src.FullyQualifiedName())
			if status != nil {
				log.Println(status.Err(), src.FullyQualifiedName())
			}
			t.SetError(ctx, err, "job.Wait")
		}
		return err
	}

	log.Println("Completed deduplication from", src.FullyQualifiedName())

	// Clear the job ID.  It won't be pushed to datastore until an error or update.
	t.JobID = ""

	// Sanity check and copy things to final DS.
	// ==========================================================================
	// Skip SanityCheckAndCopy for deployments that do not specify the final dataset.
	if rex.BQFinalDataset == "" {
		return nil
	}
	if rex.BQBatchDataset == rex.BQFinalDataset {
		// We cannot sanity check a dataset to itself.
		t.SetError(ctx, ErrDatasetNamesMatch, "Cannot SanityCheckAndCopy to same dataset")
		return ErrDatasetNamesMatch
	}
	destDs, err := rex.GetFinalDS(ctx)
	if err != nil {
		t.SetError(ctx, err, "GetFinalDS")
		return err
	}

	// Destination table has the same ID as source.
	dest := destDs.Table(copy.TableID())
	log.Printf("Sanity checking dedup'd data before final copy from %s to %s",
		copy.FullyQualifiedName(), dest.FullyQualifiedName())

	// TODO: how long can this actually take???
	copyCtx, cf := context.WithTimeout(ctx, 30*time.Minute)
	defer cf()

	srcAt := bq.NewAnnotatedTable(copy, &srcDs)
	destAt := bq.NewAnnotatedTable(dest, &destDs)

	// Copy to Final Dataset tables.
	err = bq.SanityCheckAndCopy(copyCtx, srcAt, destAt)
	if err != nil {
		t.SetError(ctx, err, "SanityCheckAndCopy")
		return err
	}

	// Delete templated dedup source table.
	log.Println("Deleting dedup source", src.FullyQualifiedName())
	// If deduplication was successful, we should delete the source table.
	delCtx, cf := context.WithTimeout(ctx, time.Minute)
	defer cf()
	err = src.Delete(delCtx)
	if err != nil {
		log.Println(err)
		t.SetError(ctx, err, "TableDelete")
		return err
	}

	return nil
}
