package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

// Dedup related errors.
var (
	ErrTableNotFound = errors.New("Table not found")
)

// DedupHandler handles requests to dedup a table.
type DedupHandler struct {
	Project      string
	Dataset      string
	MsgChan      chan state.Task
	ResponseChan chan error
}

// Sink returns the sink channel, for use by the sender.
func (dh *DedupHandler) Sink() chan<- state.Task {
	return dh.MsgChan
}

// Response returns the response channel, that closes when all processing is complete.
func (dh *DedupHandler) Response() <-chan error {
	return dh.ResponseChan
}

// WaitForStableTable loops checking until table exists and has no streaming buffer.
func WaitForStableTable(tt *bigquery.Table) error {
	log.Println("Wait for table ready", tt.FullyQualifiedName())
	var err error
	var meta *bigquery.TableMetadata
	errorDeadline := time.Now().Add(2 * time.Minute)
ErrorTimeout:
	// Check table status until streaming buffer is empty, OR there is
	// an error condition we don't expect to recover from.
	for {
		ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
		defer cf()
		meta, err = tt.Metadata(ctx)
		if err == nil && ctx.Err() != nil {
			// Convert context timeout into regular error.
			err = ctx.Err()
		}
		switch {
		case err == nil:
			// Restart the timer whenever Metadata succeeds.
			errorDeadline = time.Now().Add(2 * time.Minute)
			if meta.StreamingBuffer == nil {
				// Buffer is empty, so we can move on.
				return nil
			}
			// Otherwise just wait and check again.
		case err == io.EOF:
			// EOF is usually due to using a fake test client, so
			// treat it as a success.
			log.Println("EOF error - is this a test client?")
			return nil
		default:
			// For any error, just retry until success or timeout.
			if time.Now().After(errorDeadline) {
				// If still getting errors after two minutes, give up.
				break ErrorTimeout
			}
			// Otherwise just wait and try again.
		}
		time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
	}

	// If we fall through here, then there is some problem...
	if strings.Contains(err.Error(), "Not found: Table") {
		log.Println("Timeout waiting for table creation:", tt.FullyQualifiedName())
		metrics.FailCount.WithLabelValues("TableNotFoundTimeout")
		return ErrTableNotFound
	}
	// We are seeing occasional Error 500: An internal error ...
	log.Println(err, tt.FullyQualifiedName())
	metrics.FailCount.WithLabelValues("TableMetaErr")
	return err
}

// processOneRequest waits on the channel for a new request, and handles it.
func (dh *DedupHandler) waitAndDedup(ds *bqext.Dataset, task state.Task, clientOpts ...option.ClientOption) error {
	parts, err := tq.ParsePrefix(task.Name)
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		// TODO update metric
		metrics.FailCount.WithLabelValues("BadDedupPrefix")
		return err
	}

	// First wait for the source table's streaming buffer to be integrated.
	// This often takes an hour or more.
	tt := ds.Table(parts[2] + "_" + strings.Join(strings.Split(parts[3], "/"), ""))
	err = WaitForStableTable(tt)
	if err != nil {
		return err
	}

	// Now deduplicate the table.  NOTE that this will overwrite the destination partition
	// if it still exists.
	dest := ds.Table(parts[2] + "$" + strings.Join(strings.Split(parts[3], "/"), ""))
	log.Println("Dedupping", tt.FullyQualifiedName())
	status, err := DedupAndWait(ds, tt.TableID, dest)
	if err != nil {
		if err == io.EOF {
			log.Println("EOF error - is this a test client?")
		} else {
			log.Println(err, tt.FullyQualifiedName())
			metrics.FailCount.WithLabelValues("DedupFailed")
			return err
		}
	} else if status.Err() != nil {
		log.Println(status.Err(), tt.FullyQualifiedName())
		metrics.FailCount.WithLabelValues("DedupError")
		return err
	}

	log.Println("Completed deduplication, deleting", tt.FullyQualifiedName())
	// If deduplication was successful, we should delete the source table.
	ctx, cf := context.WithTimeout(context.Background(), time.Minute)
	defer cf()
	err = tt.Delete(ctx)
	if err != nil {
		metrics.FailCount.WithLabelValues("TableDeleteErr")
		log.Println(err)
	}
	if ctx.Err() != nil {
		if ctx.Err() != context.DeadlineExceeded {
			metrics.FailCount.WithLabelValues("TableDeleteTimeout")
			log.Println(ctx.Err())
			return err
		}
	}

	log.Println("Deleted", tt.FullyQualifiedName())
	return nil
}

// handleLoop processes requests on input channel
func (dh *DedupHandler) handleLoop(opts ...option.ClientOption) {
	log.Println("Starting handler for ...")

	for {
		task, more := <-dh.MsgChan
		if more {
			ds, err := bqext.NewDataset(dh.Project, dh.Dataset, opts...)
			if err != nil {
				metrics.FailCount.WithLabelValues("NewDataset")
				log.Println(err)
				continue
			}

			dh.waitAndDedup(&ds, task, opts...)
		} else {
			log.Println("Exiting handler for ...")
			close(dh.ResponseChan)
			break
		}
	}
}

// NewDedupHandler creates a new QueueHandler, sets up a go routine to feed it
// from a channel.
// Returns feeding channel, and done channel, which will return true when
// feeding channel is closed, and processing is complete.
func NewDedupHandler(opts ...option.ClientOption) *DedupHandler {
	project := os.Getenv("PROJECT")
	// When running in prod, the task files and queues are in mlab-oti, but the destination
	// BigQuery tables are in measurement-lab.
	if project == "mlab-oti" {
		project = "measurement-lab" // destination for production tables.
	}
	dataset := os.Getenv("DATASET")
	msg := make(chan state.Task)
	rsp := make(chan error)
	dh := DedupHandler{project, dataset, msg, rsp}

	go dh.handleLoop(opts...)
	return &dh
}

// This template expects to be executed on a table containing a single day's data, such
// as measurement-lab:batch.ndt_20170601.
//
// Some tests are collected as both uncompressed and compressed files. In some historical
// archives (June 2017), files for a single test appear in different tar files, which
// results in duplicate rows.
// This query strips the gz, finds duplicates, and chooses the best row -  prefering gzipped
// files, and prefering later parse_time.
var dedupTemplateNDT = `
	#standardSQL
	# Delete all duplicate rows based on test_id, preferring gz over non-gz, later parse_time
	SELECT * except (row_number, gz, stripped_id)
    from (
		select *, ROW_NUMBER() OVER (PARTITION BY stripped_id order by gz DESC, parse_time DESC) row_number
        FROM (
	        SELECT *, regexp_replace(test_id, ".gz$", "") as stripped_id, regexp_extract(test_id, ".*(.gz)$") as gz
	        FROM ` + "`%s`" + `
        )
    )
	WHERE row_number = 1`

// TODO - add selection by latest parse_time.
var dedupTemplateSidestream = `
	#standardSQL
	# Select single row based on test_id, 5-tuple, start-time
	SELECT * EXCEPT (row_number)
    FROM ( SELECT *, ROW_NUMBER() OVER (
        PARTITION BY CONCAT(test_id, cast(web100_log_entry.snap.StartTimeStamp as string),
            web100_log_entry.connection_spec.local_ip, cast(web100_log_entry.connection_spec.local_port as string),
            web100_log_entry.connection_spec.remote_ip, cast(web100_log_entry.connection_spec.remote_port as string))
		) row_number
	    FROM ` + "`%s`" + `)
	WHERE row_number = 1`

// Dedup executes a query that dedups and writes to destination partition.
// This function is alpha status.  The interface may change without notice
// or major version number change.
//
// `src` is relative to the project:dataset of dsExt.
// `destTable` specifies the table to write to, typically created with
//   dsExt.BqClient.DatasetInProject(...).Table(...)
//
// NOTE: If destination table is partitioned, destTable MUST include the partition
// suffix to avoid accidentally overwriting the entire table.
func Dedup(dsExt *bqext.Dataset, src string, destTable *bigquery.Table) (*bigquery.Job, error) {
	if !strings.Contains(destTable.TableID, "$") {
		meta, err := destTable.Metadata(context.Background())
		if err == nil && meta.TimePartitioning != nil {
			log.Println(err)
			metrics.FailCount.WithLabelValues("BadDestTable")
			return nil, errors.New("Destination table must specify partition")
		}
	}

	log.Printf("Removing dups and writing to %s.%s\n", destTable.DatasetID, destTable.TableID)
	var queryString string
	switch {
	case strings.HasPrefix(destTable.TableID, "sidestream"):
		queryString = fmt.Sprintf(dedupTemplateSidestream, src)
	case strings.HasPrefix(destTable.TableID, "ndt"):
		queryString = fmt.Sprintf(dedupTemplateNDT, src)
	default:
		metrics.FailCount.WithLabelValues("UnknownTableType")
		return nil, errors.New("Only handles sidestream, ndt, not " + destTable.TableID)
	}
	query := dsExt.DestQuery(queryString, destTable, bigquery.WriteTruncate)

	if query.QueryConfig.Dst == nil && query.QueryConfig.DryRun == false {
		return nil, errors.New("query must be a destination or dry run")
	}
	job, err := query.Run(context.Background())
	if err != nil {
		return nil, err
	}
	return job, nil
}

// DedupAndWait executes a query that dedups and writes to destination partition.
// Waits for query completion and returns JobStatus
func DedupAndWait(dsExt *bqext.Dataset, src string, destTable *bigquery.Table) (*bigquery.JobStatus, error) {
	job, err := Dedup(dsExt, src, destTable)
	if err != nil {
		return nil, err
	}
	return job.Wait(context.Background())
}
