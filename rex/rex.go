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
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/cloud/tq"
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

// GetBatchDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetBatchDS(ctx context.Context) (bqext.Dataset, error) {
	// TODO - bqext should use the provided context.
	return bqext.NewDataset(rex.BQProject, rex.BQBatchDataset, rex.Options...)
}

// GetFinalDS constructs an appropriate Dataset for BQ operations.
func (rex *ReprocessingExecutor) GetFinalDS(ctx context.Context) (bqext.Dataset, error) {
	// TODO - bqext should use the provided context.
	return bqext.NewDataset(rex.BQProject, rex.BQFinalDataset, rex.Options...)
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
		n, err := rex.queue(ctx, t)
		if err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.queue")
			return err
		}
		if n < 1 {
			// If there were no tasks posted, then there is nothing more to do.
			t.Queue = ""
			t.Update(ctx, state.Done)
		} else {
			t.Update(ctx, state.Processing)
		}

	case state.Processing: // TODO should this be Parsing?
		if err := rex.waitForParsing(ctx, t, terminate); err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.waitForParsing")
			return err
		}
		t.Queue = "" // No longer need to keep the queue.
		t.Update(ctx, state.Stabilizing)

	case state.Stabilizing:
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
			if !env.TestMode || err != state.ErrTableNotFound {
				// SetError also pushes to datastore, like Update(ctx, )
				t.SetError(ctx, err, "bq.WaitForStableTable")
				return err
			}
		}

		t.Update(ctx, state.Deduplicating)

	case state.Deduplicating:
		if err := rex.dedup(ctx, t); err != nil {
			// SetError also pushes to datastore, like Update(ctx, )
			t.SetError(ctx, err, "rex.dedup")
			return err
		}
		t.Update(ctx, state.Finishing)

	case state.Finishing:
		log.Println("Finishing")
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
	for err := qh.IsEmpty(); err != nil; err = qh.IsEmpty() {
		select {
		case <-terminate:
			t.SetError(ctx, err, "Terminating")
			return err
		default:
		}
		if err == tq.ErrMoreTasks {
			// Wait 5-15 seconds before checking again.
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		} else if err != nil {
			if err == io.EOF && env.TestMode {
				// Expected when using test client.
				return nil
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
	return nil
}

func (rex *ReprocessingExecutor) queue(ctx context.Context, t *state.Task) (int, error) {
	// Submit all files from the bucket that match the prefix.
	// Where do we get the bucket?
	//func (qh *ChannelQueueHandler) handleLoop(next api.BasicPipe, bucketOpts ...option.ClientOption) {
	qh, err := tq.NewQueueHandler(rex.Config, t.Queue)
	if err != nil {
		t.SetError(ctx, err, "NewQueueHandler")
		return 0, err
	}
	parts, err := t.ParsePrefix()
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		t.SetError(ctx, err, "BadPrefix")
		return 0, err
	}
	bucketName := parts[0]

	// Use a real storage bucket.
	// TODO - add a persistent storageClient to the rex object?
	sc, err := storage.NewClient(ctx, rex.BucketOpts...)
	if err != nil {
		log.Println(err)
		t.SetError(ctx, err, "StorageClientError")
		return 0, err
	}
	storageClient := stiface.AdaptClient(sc)
	// TODO - try cancelling the context instead?
	defer storageClient.Close()
	bucket, err := tq.GetBucket(ctx, storageClient, rex.Project, bucketName, false)
	if err != nil {
		if err == io.EOF && env.TestMode {
			log.Println("Using fake client, ignoring EOF error")
			return 0, nil
		}
		log.Println(err)
		t.SetError(ctx, err, "BucketError")
		return 0, err
	}
	// NOTE: This does not check the terminate channel, so once started, it will
	// complete the queuing.
	n, err := qh.PostDay(ctx, bucket, bucketName, parts[1]+"/"+parts[2]+"/")
	if err != nil {
		log.Println(err)
		t.SetError(ctx, err, "PostDayError")
		return n, nil
	}
	log.Println("Added ", n, t.Name, " tasks to ", qh.Queue)
	return n, nil
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
	job, err := bq.Dedup(ctx, &ds, src.TableID, dest)
	if err != nil {
		if err == io.EOF {
			if env.TestMode {
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
			log.Println(err)
		} else if status.Err() != nil {
			log.Println(job.ID(), status.Err())
			if strings.Contains(status.Err().Error(), "Not found: Table") {
				return state.ErrTableNotFound
			}
			if strings.Contains(status.Err().Error(), "rows belong to different partitions") {
				return state.ErrRowsFromOtherPartition
			}
			if backoff == maxBackoff {
				return status.Err()
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
	err = waitForJob(ctx, job, 60*time.Second, terminate)
	if err != nil {
		log.Println(err, src.FullyQualifiedName())
		t.SetError(ctx, err, "waitForJob")
		return err
	}
	// TODO - should this context have a deadline?
	status, err := job.Wait(ctx)
	if err != nil {
		if err != state.ErrTaskSuspended {
			log.Println(status.Err(), src.FullyQualifiedName())
			t.SetError(ctx, err, "job.Wait")
		}
		return err
	}

	// Delete dedup source table.

	// Wait for JobID to complete, then delete the template table.
	log.Println("Completed deduplication, deleting source", src.FullyQualifiedName())
	// If deduplication was successful, we should delete the source table.
	delCtx, cf := context.WithTimeout(ctx, time.Minute)
	defer cf()
	err = src.Delete(delCtx)
	if err != nil {
		log.Println(err)
		t.SetError(ctx, err, "TableDelete")
		return err
	}

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
	dest := destDs.Table(copy.TableID)
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
	return nil
}
