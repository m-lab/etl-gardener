package bq

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/bqext"
)

// testMode is set IFF the test.v flag is defined, as it is in all go testing.T tests.
// Use with caution!
var testMode bool

func init() {
	testMode = flag.Lookup("test.v") != nil
}

// Dedup related errors.
var (
	ErrTableNotFound = errors.New("Table not found")
)

// WaitForStableTable loops checking until table exists and has no streaming buffer.
// TODO - move these functions to go/bqext package
func WaitForStableTable(ctx context.Context, tt *bigquery.Table) error {
	errorTimeout := 2 * time.Minute
	if testMode {
		errorTimeout = 100 * time.Millisecond
	}
	errorDeadline := time.Now().Add(errorTimeout)
	log.Println("Wait for table ready", tt.FullyQualifiedName())
	var err error
	var meta *bigquery.TableMetadata
ErrorTimeout:
	// Check table status until streaming buffer is empty, OR there is
	// an error condition we don't expect to recover from.
	for {
		ctx, cf := context.WithTimeout(ctx, 10*time.Second)
		defer cf()
		meta, err = tt.Metadata(ctx)
		if err == nil && ctx.Err() != nil {
			// Convert context timeout into regular error.
			err = ctx.Err()
		}

		switch {
		case err == nil:
			// Restart the timer whenever Metadata succeeds.
			errorDeadline = time.Now().Add(errorTimeout)
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

// This template expects to be executed on a table containing a single day's data, such
// as mlab-oti:batch.ndt_20170601.
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

// dedupTemplateSwitch expects to run on a table with a single day's data, i.e.
// "mlab-sandbox.batch.switch_20170601". The query ignores duplicate rows as
// determined by unique combinations of test_id (which includes collection
// timestamps), metric name, hostname, and experiment name. The `sample`
// repeated record is copied as-is from the first row.
var dedupTemplateSwitch = `
	#standardSQL
	SELECT
		* EXCEPT (row_number)
	FROM (
		SELECT
			*, ROW_NUMBER() OVER (
				PARTITION BY CONCAT(test_id, metric, hostname, experiment)
			) AS row_number
		FROM ` + "`%s`" + `
	)
	WHERE
		row_number = 1`

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
// TODO - move these functions to go/bqext package
// TODO - should we get the context from the dsExt?
func Dedup(ctx context.Context, dsExt *bqext.Dataset, src string, destTable *bigquery.Table) (*bigquery.Job, error) {
	if !strings.Contains(destTable.TableID, "$") {
		meta, err := destTable.Metadata(ctx)
		if err == nil && meta.TimePartitioning != nil {
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
	case strings.HasPrefix(destTable.TableID, "switch"):
		queryString = fmt.Sprintf(dedupTemplateSwitch, src)
	default:
		log.Println("Only handles sidestream, ndt, switch, not " + destTable.TableID)
		return nil, errors.New("Unknown table type")
	}
	query := dsExt.DestQuery(queryString, destTable, bigquery.WriteTruncate)

	if query.QueryConfig.Dst == nil && query.QueryConfig.DryRun == false {
		return nil, errors.New("query must be a destination or dry run")
	}
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	return job, nil
}
