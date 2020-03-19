package ops

import (
	"context"
	"flag"
	"io"
	"log"
	"strings"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl/etl"
)

// PartitionedTable creates BQ Table for legacy source templated table
func PartitionedTable(j tracker.Job, ds *dataset.Dataset) bqiface.Table {
	tableName := etl.DirToTablename(j.Datatype)
	log.Println(ds.Dataset, tableName)

	src := ds.Table(tableName + "$" + j.Date.Format("20060102"))
	return src
}

// TemplateTable creates BQ Table for legacy source templated table
func TemplateTable(j tracker.Job, ds *dataset.Dataset) bqiface.Table {
	tableName := etl.DirToTablename(j.Datatype)
	log.Println(ds.Dataset, tableName)

	src := ds.Table(tableName + "_" + j.Date.Format("20060102"))
	return src
}

// This is a function I didn't want to port to the new architecture.  8-(
func (m *Monitor) waitForStableTable(ctx context.Context, j tracker.Job) error {
	log.Println("Stabilizing:", j)
	// Wait for the streaming buffer to be nil.

	src := TemplateTable(j, &m.batchDS)
	err := bq.WaitForStableTable(ctx, src)
	if err != nil {
		// When testing, we expect to get ErrTableNotFound here.
		if err != state.ErrTableNotFound {
			return err
		}
	}

	return nil
}

func isTest() bool {
	return flag.Lookup("test.v") != nil
}

func newStateFunc(state tracker.State, detail string) ActionFunc {
	return func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
		log.Println(j, state)
		metrics.StateDate.WithLabelValues(j.Datatype, string(state)).SetToCurrentTime()
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
		newStateFunc(tracker.Stabilizing, "-"),
		"Changing to Stabilizing")
	m.AddAction(tracker.Stabilizing,
		// TODO - determine whether we need to stabilize or can just go ahead with the dedup
		// query, which should capture the streaming buffer.
		func(ctx context.Context, j tracker.Job) bool {
			return m.waitForStableTable(ctx, j) == nil
		},
		newStateFunc(tracker.Deduplicating, "-"),
		"Stabilizing")
	m.AddAction(tracker.Deduplicating,
		// HACK
		nil,
		func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
			start := time.Now()
			// TODO - pass tracker to dedup, so dedup can record the JobID.
			err := m.dedup(ctx, j)
			if err != nil {
				if err == state.ErrBQRateLimitExceeded {
					return // Should try again
				}
				log.Println(err)
				tk.SetJobError(j, err.Error())
				return
			}
			s.State = tracker.Finishing
			log.Println(j, s.State)
			metrics.StateDate.WithLabelValues(j.Datatype, string(tracker.Finishing)).SetToCurrentTime()
			err = tk.SetStatus(j, tracker.Finishing, "dedup took "+time.Since(start).Round(100*time.Millisecond).String())
			if err != nil {
				log.Println(err)
			}
		},
		"Deduplicating")
	m.AddAction(tracker.Finishing,
		nil,
		func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
			start := time.Now()
			// TODO - need to copy partition to final location.
			// TODO - pass tracker to dedup, so dedup can record the JobID.
			err := m.deleteSrc(ctx, j)
			if err != nil {
				if err == state.ErrBQRateLimitExceeded {
					return // Should try again
				}
				log.Println(err)
				tk.SetJobError(j, err.Error())
				return
			}
			s.State = tracker.Complete
			log.Println(j, s.State, time.Since(start).Round(100*time.Millisecond))
			metrics.StateDate.WithLabelValues(j.Datatype, string(tracker.Complete)).SetToCurrentTime()
			err = tk.SetStatus(j, tracker.Complete, "delete took "+time.Since(start).Round(100*time.Millisecond).String())
			if err != nil {
				log.Println(err)
			}
		},
		"Deleting template table")
	return m, nil
}

func (m *Monitor) deleteSrc(ctx context.Context, j tracker.Job) error {
	src := TemplateTable(j, &m.batchDS)

	delCtx, cf := context.WithTimeout(ctx, time.Minute)
	defer cf()
	return src.Delete(delCtx)
}

func (m *Monitor) dedup(ctx context.Context, j tracker.Job) error {
	// Launch the dedup request, and save the JobID
	src := TemplateTable(j, &m.batchDS)
	dest := PartitionedTable(j, &m.batchDS)

	log.Println("Dedupping", src.FullyQualifiedName())
	// TODO move Dedup??
	// TODO - implement backoff?
	bqJob, err := bq.Dedup(ctx, &m.batchDS, src.TableID(), dest)
	if err != nil {
		if err == io.EOF {
			if isTest() {
				bqJob, err = m.batchDS.BqClient.JobFromID(ctx, "fakeJobID")
				return nil
			}
		} else {
			log.Println(err, src.FullyQualifiedName())
			//t.SetError(ctx, err, "DedupFailed")
			return err
		}
	}
	return waitForJob(ctx, j, bqJob, time.Minute)
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
