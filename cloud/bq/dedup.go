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
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/metrics"
)

// Use with caution!
func isTest() bool {
	return flag.Lookup("test.v") != nil
}

// Dedup related errors.
var (
	ErrTableNotFound = errors.New("Table not found")
)

// WaitForStableTable loops checking until table exists and has no streaming buffer.
// NOTE: We discovered that the StreamingBuffer == nil is not reliable, and must be rechecked for a minimum
// of 5 minutes to ensure that there is no more buffered data.  (We saw reverts after as long as 4m50s).
// TODO - refactor this to make it easier to understand.
// TODO - move these functions to go/bqext package
func WaitForStableTable(ctx context.Context, tt bqiface.Table) error {
	fqn := tt.FullyQualifiedName()
	log.Println("Wait for table ready", fqn)

	never := time.Time{}
	// bufferEmptySince indicates the first time we saw nil StreamingBuffer
	bufferEmptySince := never
	// NOTE: This must be larger than the errorTimeout.
	// We have seen false negatives up to about 2.5 minutes, so 5 minutes might not be enough.
	emptyBufferWaitTime := 60 * time.Minute

	errorTimeout := 2 * time.Minute
	if isTest() {
		errorTimeout = 100 * time.Millisecond
	}
	errorDeadline := time.Now().Add(errorTimeout)
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
				if bufferEmptySince == never {
					bufferEmptySince = time.Now()
				}
				if time.Since(bufferEmptySince) > emptyBufferWaitTime {
					// We believe buffer really is empty, so we can move on.
					return nil
				}
				// Otherwise just wait and check again.
			} else {
				if bufferEmptySince != never {
					log.Println("Streaming buffer was empty for", time.Since(bufferEmptySince),
						"but now it is not!", tt.FullyQualifiedName())
					bufferEmptySince = never
				}
				// Now wait and check again.
			}
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
		metrics.FailCount.WithLabelValues(tt.DatasetID(), tt.TableID(), "TableNotFoundTimeout")
		return ErrTableNotFound
	}
	// We are seeing occasional Error 500: An internal error ...
	log.Println(err, tt.FullyQualifiedName())
	metrics.FailCount.WithLabelValues(tt.DatasetID(), tt.TableID(), "TableMetaErr")
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
		select *,
		# Prefer more snapshots, metadata, earlier task names, gzipped, later parse time
		ROW_NUMBER() OVER (PARTITION BY stripped_id ORDER BY anomalies.num_snaps DESC, anomalies.no_meta, task_filename, gz DESC, parse_time DESC) row_number
        FROM (
			SELECT *,
			    REGEXP_REPLACE(test_id, ".gz$", "") AS stripped_id,
		        REGEXP_EXTRACT(test_id, ".*(.gz)$") AS gz
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
		# Use the most recently parsed row
		ORDER BY parse_time DESC
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
			# Use the most recently parsed row
			ORDER BY parse_time DESC
			) AS row_number
		FROM ` + "`%s`" + `
	)
	WHERE
		row_number = 1`

// dedupTemplateTraceroute expects to be executed on a table containing a single day's data, such
// as mlab-oti:batch.traceroute_20170601.
var dedupTemplateTraceroute = `
	#standardSQL
	# Select single row based on TestTime, client_ip, server_ip
	SELECT * EXCEPT (row_number)
    FROM ( SELECT *, ROW_NUMBER() OVER (
        PARTITION BY CONCAT(STRING(TestTime), Source.IP, DESTINATION.IP)
		) row_number
	    FROM ` + "`%s`" + `)
	WHERE row_number = 1`

var dedupTemplateTCPInfo = `
	#standardSQL
	# Delete all duplicate rows based on uuid, preferring "later" task filename, later parse_time
	SELECT * EXCEPT (row_number)
    FROM (
		SELECT *,
		# Prefer more snapshots, earlier task names, later parse time
		ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC) row_number
        FROM (
			SELECT *
    		FROM ` + "`%s`" + `
        )
    )
	WHERE row_number = 1`

var dedupTemplateNDTResult = `
	#standardSQL
	SELECT * EXCEPT (row_number)
	FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY ParseInfo.ParseTime DESC) AS row_number
		FROM ` + "`%s`" + `
	)
	WHERE
		row_number = 1`

// Dedup executes a query that dedups and writes to destination partition.
// `src` is relative to the project:dataset of dsExt.
// `destTable` specifies the table to write to, typically created with
//   dsExt.BqClient.DatasetInProject(...).Table(...)
//
// NOTE: If destination table is partitioned, destTable MUST include the partition
// suffix to avoid accidentally overwriting the entire table.
// TODO - move these functions to go/bqext package
func Dedup(ctx context.Context, dsExt *dataset.Dataset, src string, destTable bqiface.Table) (bqiface.Job, error) {
	if !strings.Contains(destTable.TableID(), "$") {
		meta, err := destTable.Metadata(ctx)
		if err == nil && meta.TimePartitioning != nil {
			return nil, errors.New("Destination table must specify partition")
		}
	}

	log.Printf("Removing dups and writing to %s.%s\n", destTable.DatasetID(), destTable.TableID())
	var queryString string
	switch {
	case strings.HasPrefix(destTable.TableID(), "sidestream"):
		queryString = fmt.Sprintf(dedupTemplateSidestream, src)
	case strings.HasPrefix(destTable.TableID(), "ndt5") || strings.HasPrefix(destTable.TableID(), "ndt7"):
		queryString = fmt.Sprintf(dedupTemplateNDTResult, src)
	case strings.HasPrefix(destTable.TableID(), "ndt"):
		queryString = fmt.Sprintf(dedupTemplateNDT, src)
	case strings.HasPrefix(destTable.TableID(), "switch"):
		queryString = fmt.Sprintf(dedupTemplateSwitch, src)
	case strings.HasPrefix(destTable.TableID(), "traceroute"):
		queryString = fmt.Sprintf(dedupTemplateTraceroute, src)
	case strings.HasPrefix(destTable.TableID(), "tcpinfo"):
		queryString = fmt.Sprintf(dedupTemplateTCPInfo, src)
	default:
		log.Println("Only handles sidestream, ndt, switch, traceroute, not " + destTable.TableID())
		return nil, errors.New("Unknown table type")
	}
	query := dsExt.DestQuery(queryString, destTable, bigquery.WriteTruncate)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	return job, nil
}
