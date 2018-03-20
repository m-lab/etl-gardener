package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl-gardener/api"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/option"
)

// DedupHandler handles requests to dedup a table.
type DedupHandler struct {
	Project      string
	Dataset      string
	MsgChan      chan string
	ResponseChan chan error
}

// Sink returns the sink channel, for use by the sender.
func (dh *DedupHandler) Sink() chan<- string {
	return dh.MsgChan
}

// Response returns the response channel, that closes when all processing is complete.
func (dh *DedupHandler) Response() <-chan error {
	return dh.ResponseChan
}

func assertBasicPipe() {
	func(ds api.BasicPipe) {}(&DedupHandler{})
}

// waitForEmptyQueue loops checking queue until empty.
func waitForStableTable(tt *bigquery.Table) error {
	// Don't want to accept a date until we can actually queue it.
	log.Println("Wait for table ready", tt.TableID)
	time.Sleep(time.Minute)
	for {
		ctx, cf := context.WithTimeout(context.Background(), 60*time.Second)
		defer cf()
		meta, err := tt.Metadata(ctx)
		if err != nil {
			metrics.FailCount.WithLabelValues("TableMetaErr")
			log.Println(err)
			return err
		}
		if ctx.Err() != nil {
			if ctx.Err() != context.DeadlineExceeded {
				log.Println(ctx.Err())
				return err
			}
		} else if meta.StreamingBuffer == nil {
			break
		}
		// Retry roughly every 10 seconds.
		time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
	}
	return nil
}

// processOneRequest waits on the channel for a new request, and handles it.
func (dh *DedupHandler) waitAndDedup(ds *bqext.Dataset, prefix string, clientOpts ...option.ClientOption) error {
	parts, err := tq.ParsePrefix(prefix)
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
	err = waitForStableTable(tt)
	if err != nil {
		metrics.FailCount.WithLabelValues("StreamingBufferWaitFailed")
		return err
	}

	// Now deduplicate the table.  NOTE that this will overwrite the destination partition
	// if it still exists.
	dest := ds.Table(parts[2] + "$" + strings.Join(strings.Split(parts[3], "/"), ""))
	log.Println("Dedupping", tt.FullyQualifiedName())
	status, err := Dedup(ds, tt.TableID, dest)
	if err != nil {
		log.Println(err)
		metrics.FailCount.WithLabelValues("DedupFailed")
		return err
	}
	if status.Err() != nil {
		log.Println(status.Err())
		metrics.FailCount.WithLabelValues("DedupError")
		return err
	}

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
			metrics.FailCount.WithLabelValues("TableDeleteErr2")
			log.Println(ctx.Err())
			return err
		}
	}

	return nil
}

// handleLoop processes requests on input channel
func (dh *DedupHandler) handleLoop(opts ...option.ClientOption) {
	log.Println("Starting handler for ...")

	for {
		req, more := <-dh.MsgChan
		if more {
			ds, err := bqext.NewDataset(dh.Project, dh.Dataset, opts...)
			if err != nil {
				metrics.FailCount.WithLabelValues("NewDataset")
				log.Println(err)
				continue
			}

			dh.waitAndDedup(&ds, req, opts...)
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
func NewDedupHandler(queue string, opts ...option.ClientOption) *DedupHandler {
	project := os.Getenv("PROJECT")
	dataset := os.Getenv("DATASET")
	msg := make(chan string)
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
func Dedup(dsExt *bqext.Dataset, src string, destTable *bigquery.Table) (*bigquery.JobStatus, error) {
	if !strings.Contains(destTable.TableID, "$") {
		meta, err := destTable.Metadata(context.Background())
		if err == nil && meta.TimePartitioning != nil {
			log.Println(err)
			metrics.FailCount.WithLabelValues("BadDestTable")
			return nil, errors.New("Destination table must specify partition")
		}
	}

	log.Printf("Removing dups and writing to %s.%s\n", destTable.DatasetID, destTable.TableID)
	switch {
	case strings.HasPrefix(destTable.TableID, "sidestream"):
		queryString := fmt.Sprintf(dedupTemplateSidestream, src)
		query := dsExt.DestQuery(queryString, destTable, bigquery.WriteTruncate)
		return dsExt.ExecDestQuery(query)
	case strings.HasPrefix(destTable.TableID, "ndt"):
		queryString := fmt.Sprintf(dedupTemplateNDT, src)
		query := dsExt.DestQuery(queryString, destTable, bigquery.WriteTruncate)
		return dsExt.ExecDestQuery(query)
	default:
		metrics.FailCount.WithLabelValues("UnknownTableType")
		return nil, errors.New("Only handles sidestream, ndt, not " + destTable.TableID)
	}
}
