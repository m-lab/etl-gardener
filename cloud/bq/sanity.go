// Package bq provides facilities for sanity checking and
// copying final table into a destination partition.
// It is currently somewhat NDT specific:
//  1. It expects tables to have task_filename field.
//  2. It expects destination table to be partitioned.
//  3. It does not explicitly check for schema compatibility,
//      though it will fail if they are incompatible.
package bq

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/m-lab/go/dataset"
)

var (
	// ErrNilContext is returned when a request could not be completed with
	// a nil context.
	ErrNilContext = errors.New("Could not be completed without context")
	// ErrNotRegularTable is returned when a table is not a regular table (e.g. views)
	ErrNotRegularTable = errors.New("Not a regular table")
	// ErrNoDataset is returned when a operation requires a dataset, but none is provided/available.
	ErrNoDataset = errors.New("No dataset available")
	// ErrSrcOlderThanDest is returned if a source table is older than the destination partition.
	ErrSrcOlderThanDest = errors.New("Source older than destination partition")
	// ErrTooFewTasks is returned when the source table has fewer task files than the destination.
	ErrTooFewTasks = errors.New("Too few tasks")
	// ErrTooFewTests is returned when the source table has fewer tests than the destination.
	ErrTooFewTests = errors.New("Too few tests")
	// ErrMismatchedPartitions is returned when partition dates should match but don't.
	ErrMismatchedPartitions = errors.New("Partition dates don't match")
)

// Detail provides more detailed information about a partition or table.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// AnnotatedTable binds a bigquery.Table with associated additional info.
// It keeps track of all the info we have so far, automatically fetches
// more data as needed, and thus avoids possibly fetching multiple times.
type AnnotatedTable struct {
	bqiface.Table
	dataset *dataset.Dataset // A dataset that can query the table.  May be nil.
	meta    *bigquery.TableMetadata
	detail  *Detail
	pInfo   *dataset.PartitionInfo
	err     error // first error (if any) when attempting to fetch annotation
}

// NewAnnotatedTable creates an AnnotatedTable
func NewAnnotatedTable(t bqiface.Table, ds *dataset.Dataset) *AnnotatedTable {
	return &AnnotatedTable{Table: t, dataset: ds}
}

// CachedMeta returns metadata if available.
// If ctx is non-nil, will attempt to fetch metadata if it is not already cached.
// Returns error if meta not available.
func (at *AnnotatedTable) CachedMeta(ctx context.Context) (*bigquery.TableMetadata, error) {
	if at.meta != nil {
		return at.meta, nil
	}
	if at.err != nil {
		return nil, at.err
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	at.meta, at.err = at.Metadata(ctx)
	return at.meta, at.err
}

// LastModifiedTime returns the LMT field from the metadata.  If not
// available, returns time.Time{} which is the zero time.
// Caller should take care if missing metadata might be a problem.
func (at *AnnotatedTable) LastModifiedTime(ctx context.Context) time.Time {
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		return time.Time{} // Default time.  Caller might need to check for this.
	}
	return meta.LastModifiedTime
}

// CachedDetail returns the cached detail, or fetches it if possible.
func (at *AnnotatedTable) CachedDetail(ctx context.Context) (*Detail, error) {
	if at.detail != nil {
		return at.detail, nil
	}
	if at.err != nil {
		return nil, at.err
	}
	if at.dataset == nil {
		return nil, ErrNoDataset
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	at.detail, at.err = GetTableDetail(ctx, at.dataset, at.Table)
	if at.err != nil {
		log.Println(at.FullyQualifiedName(), at.TableID())
	}
	return at.detail, at.err
}

// CachedPartitionInfo returns the cached PInfo, possibly fetching it if possible.
func (at *AnnotatedTable) CachedPartitionInfo(ctx context.Context) (*dataset.PartitionInfo, error) {
	if at.pInfo != nil {
		return at.pInfo, nil
	}
	if at.err != nil {
		return nil, at.err
	}
	if at.dataset == nil {
		return nil, ErrNoDataset
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	at.pInfo, at.err = at.GetPartitionInfo(ctx)
	return at.pInfo, at.err
}

// CheckIsRegular returns nil if table exists and is a regular table.
// Otherwise returns an error, indicating
// If metadata is not available, returns false and associated error.
func (at *AnnotatedTable) CheckIsRegular(ctx context.Context) error {
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		return err
	}
	if meta.Type != bigquery.RegularTable {
		return ErrNotRegularTable
	}
	return nil
}

// GetTableDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, and task_filename fields for legacy tables,
// but it is not true for new traceroute tables.
func GetTableDetail(ctx context.Context, dsExt *dataset.Dataset, table bqiface.Table) (*Detail, error) {
	// If table is a partition, then we have to separate out the partition part for the query.
	parts := strings.Split(table.TableID(), "$")
	dataset := table.DatasetID()
	tableName := parts[0]
	where := ""
	if len(parts) > 1 {
		if len(parts[1]) == 8 {
			where = "where _PARTITIONTIME = PARSE_TIMESTAMP(\"%Y%m%d\",\"" + parts[1] + "\")"
		} else {
			return nil, errors.New("Invalid partition string: " + parts[1])
		}
	}
	detail := Detail{}
	legacyQuery := fmt.Sprintf(`
		#standardSQL
		SELECT COUNT(DISTINCT test_id) AS TestCount, COUNT(DISTINCT task_filename) AS TaskFileCount
    FROM `+"`%s.%s`"+`
		%s  -- where clause`,
		dataset, tableName, where)

	tcpinfoQuery := fmt.Sprintf(`
		#standardSQL
		SELECT COUNT(DISTINCT UUID) AS TestCount, COUNT(DISTINCT ParseInfo.TaskFileName) AS TaskFileCount
    FROM `+"`%s.%s`"+`
		%s  -- where clause`,
		dataset, tableName, where)

	tracerouteQuery := fmt.Sprintf(`
		#standardSQL
		SELECT COUNT(DISTINCT ParseInfo.TaskFileName) AS TestCount, COUNT(DISTINCT ParseInfo.TaskFileName) AS TaskFileCount
    FROM `+"`%s.%s`"+`
		%s  -- where clause`,
		dataset, tableName, where)

	resultNDTQuery := fmt.Sprintf(`
		#standardSQL
		SELECT COUNT(DISTINCT test_id) AS TestCount, COUNT(DISTINCT ParseInfo.TaskFileName) AS TaskFileCount
    FROM `+"`%s.%s`"+`
		%s  -- where clause`,
		dataset, tableName, where)

	// TODO - find a better way to do this.
	// https://github.com/m-lab/etl-gardener/issues/158
	query := legacyQuery
	if parts[0] == "tcpinfo" {
		query = tcpinfoQuery
	} else if (parts[0] == "ndt5") || (parts[0] == "ndt7") {
		query = resultNDTQuery
	} else if parts[0] == "traceroute" {
		query = tracerouteQuery
	}
	err := dsExt.QueryAndParse(ctx, query, &detail)
	if err != nil {
		log.Println(err)
		log.Println("Query:", query)
	}
	return &detail, err
}

// GetTablesMatching finds all tables matching table filter
// and collects the basic stats about each of them.
// It performs many network operations, possibly two per table.
// Returns slice ordered by decreasing age.
func GetTablesMatching(ctx context.Context, dsExt *dataset.Dataset, filter string) ([]AnnotatedTable, error) {
	alt := make([]AnnotatedTable, 0)
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID(), filter) {
			// TODO - make this run in parallel
			at := AnnotatedTable{Table: t, dataset: dsExt}
			_, err := at.CachedMeta(ctx)
			if err == ErrNotRegularTable {
				continue
			}
			if err != nil {
				return nil, err
			}
			alt = append(alt, at)
		}
	}
	sort.Slice(alt[:], func(i, j int) bool {
		return alt[i].LastModifiedTime(ctx).Before(alt[j].LastModifiedTime(ctx))
	})
	return alt, nil
}

// YYYYMMDD matches valid dense year/month/date strings
const YYYYMMDD = `\d{4}[01]\d[0123]\d`

var denseDateSuffix = regexp.MustCompile(`(.*)([_$])(` + YYYYMMDD + `)$`)

// tableNameParts is used to describe a templated table or table partition.
type tableNameParts struct {
	prefix        string
	isPartitioned bool
	yyyymmdd      string
}

// getTableParts separates a table name into prefix/base, separator, and partition date.
// If tableName does not include valid yyyymmdd suffix, returns an error.
func getTableParts(tableName string) (tableNameParts, error) {
	date := denseDateSuffix.FindStringSubmatch(tableName)
	if len(date) != 4 || len(date[3]) != 8 {
		return tableNameParts{}, errors.New("Invalid template suffix: " + tableName)
	}
	return tableNameParts{date[1], date[2] == "$", date[3]}, nil
}

// GetPartitionInfo provides basic information about a partition.
// Unlike dataset.GetPartitionInfo, this gets project and dataset from the
// table, which should include partition spec.
// at.dataset should have access to the table, but its project and dataset are not used.
// TODO - possibly migrate this to go/dataset.
func (at *AnnotatedTable) GetPartitionInfo(ctx context.Context) (*dataset.PartitionInfo, error) {
	tableName := at.Table.TableID()
	parts, err := getTableParts(tableName)
	if err != nil {
		return nil, err
	}
	if !parts.isPartitioned {
		return nil, errors.New("TableID does not specify partition: " + tableName)
	}
	// Assemble the FQ table name, without the partition suffix.
	fullTable := fmt.Sprintf("%s:%s.%s", at.ProjectID(), at.DatasetID(), parts.prefix)

	// This uses legacy, because PARTITION_SUMMARY is not supported in standard.
	queryString := fmt.Sprintf(
		`#legacySQL
		SELECT
		  partition_id AS PartitionID,
		  MSEC_TO_TIMESTAMP(creation_time) AS CreationTime,
		  MSEC_TO_TIMESTAMP(last_modified_time) AS LastModified
		FROM
		  [%s$__PARTITIONS_SUMMARY__]
		WHERE partition_id = "%s" `, fullTable, parts.yyyymmdd)
	pInfo := dataset.PartitionInfo{}

	err = at.dataset.QueryAndParse(ctx, queryString, &pInfo)
	if err != nil {
		// If the partition doesn't exist, just return empty Info, no error.
		if err == iterator.Done {
			return &dataset.PartitionInfo{}, nil
		}
		return nil, err
	}
	return &pInfo, nil
}

// IncludeTaskFileCountCheck temporarily disables the task file count check, to address the problem
// with 2012.
const IncludeTaskFileCountCheck = false
const testCountRequirement = 0.99 // Query updated to count DISTINCT test_ids, so this can now be much tighter.
const taskCountRequirement = 0.99

// checkAlmostAsBig compares the current and given AnnotatedTable test counts and
// task file counts. When the current AnnotatedTable has more than 1% fewer task files or 5%
// fewer rows compare to the given AnnotatedTable, then a descriptive error is returned.
func (at *AnnotatedTable) checkAlmostAsBig(ctx context.Context, other *AnnotatedTable) error {
	thisDetail, err := at.CachedDetail(ctx)
	if err != nil {
		return err
	}
	otherDetail, err := other.CachedDetail(ctx)
	if err != nil {
		return err
	}

	// Check that receiver table contains at least 99% as many tasks as
	// other table.
	if thisDetail.TaskFileCount < otherDetail.TaskFileCount {
		log.Printf("Warning - fewer task files: %s(%d) < %s(%d) possibly due to redundant task files.\n",
			at.Table.FullyQualifiedName(), thisDetail.TaskFileCount,
			other.Table.FullyQualifiedName(), otherDetail.TaskFileCount)
	}

	// NOTE: We have discovered that in 2012, some archives contain tests that are entirely
	// redundant with tests in other archives.  This means that some archives are completely removed
	// in the dedup process.  Since these archives appear in the original "base_tables", this check
	// has been causing the sanity check to fail.
	if IncludeTaskFileCountCheck && float32(thisDetail.TaskFileCount) < taskCountRequirement*float32(otherDetail.TaskFileCount) {
		return ErrTooFewTasks
	}

	// Check that receiver table contains at least 95% as many tests as
	// other table.  This may be fewer if the destination table still has dups.
	if thisDetail.TestCount < otherDetail.TestCount {
		log.Printf("Warning - fewer tests: %s(%d) < %s(%d)\n",
			at.Table.FullyQualifiedName(), thisDetail.TestCount,
			other.Table.FullyQualifiedName(), otherDetail.TestCount)
	}
	// We are now using DISTINCT test counts, so we can use a tighter bound.
	if float32(thisDetail.TestCount) < testCountRequirement*float32(otherDetail.TestCount) {
		return ErrTooFewTests
	}
	return nil
}

// TODO - should we use the metadata, or the PI?  Are they the same?
func (at *AnnotatedTable) checkModifiedAfter(ctx context.Context, other *AnnotatedTable) error {
	// If the source table is older than the destination table, then
	// don't overwrite it.
	thisMeta, err := at.CachedMeta(ctx)
	if err != nil {
		return err
	}
	// Note that if other doesn't actually exist, its LastModifiedTime will be the time zero value,
	// so this will generally work as intended.
	if thisMeta.LastModifiedTime.Before(other.LastModifiedTime(ctx)) {
		// TODO should perhaps delete the source table?
		return ErrSrcOlderThanDest
	}
	return nil
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - move this to go/dataset, since it is bigquery specific and general purpose.
func WaitForJob(ctx context.Context, job bqiface.Job, maxBackoff time.Duration) error {
	backoff := 10 * time.Millisecond
	previous := backoff
	for {
		status, err := job.Status(ctx)
		if err != nil {
			return err
		}
		if status.Done() {
			if status.Err() != nil {
				return status.Err()
			}
			break
		}
		if backoff < maxBackoff {
			tmp := previous
			previous = backoff
			backoff = backoff + tmp
		}
		time.Sleep(backoff)
	}
	return nil
}

// SanityCheckAndCopy uses several sanity checks to improve copy safety.
// Caller should also have checked source and destination ages, and task/test counts.
//  1. Source is required to be a single partition or templated table with yyyymmdd suffix.
//  2. Destination partition matches source partition/suffix
// TODO(gfr) Ideally this should be done by a separate process with
// higher priviledge than the reprocessing and dedupping processes.
// TODO(gfr) Also support copying from a template instead of partition?
func SanityCheckAndCopy(ctx context.Context, src, dest *AnnotatedTable) error {
	// Extract the
	srcParts, err := getTableParts(src.TableID())
	if err != nil {
		return err
	}

	destParts, err := getTableParts(dest.TableID())
	if err != nil {
		return err
	}
	if destParts.yyyymmdd != srcParts.yyyymmdd {
		return ErrMismatchedPartitions
	}

	err = src.checkAlmostAsBig(ctx, dest)
	if err != nil {
		return err
	}

	err = src.checkModifiedAfter(ctx, dest)
	if err != nil {
		// TODO: Should we delete the source table here?
		log.Printf("%s modified (%v) after %s (%v)\n", src.FullyQualifiedName(), src.LastModifiedTime(ctx), dest.FullyQualifiedName(), dest.LastModifiedTime(ctx))
		return err
	}

	copier := dest.Table.CopierFrom(src.Table)
	config := bqiface.CopyConfig{}
	config.WriteDisposition = bigquery.WriteTruncate
	config.Dst = dest.Table
	config.Srcs = append(config.Srcs, src.Table)
	copier.SetCopyConfig(config)
	job, err := copier.Run(ctx)
	if err != nil {
		log.Println("Error Copying...", src.TableID(), "error:", err)
		return err
	}
	log.Println("Start Copying...", src.TableID(), "JobID:", job.ID())

	err = WaitForJob(ctx, job, 10*time.Second)
	if err != nil {
		log.Println("Error Waiting...", src.TableID(), "JobID:", job.ID(), "error:", err)
	} else {
		log.Println("Done Copying...", src.TableID(), "JobID:", job.ID())
	}
	log.Println("SanityCheckAndCopy Done")
	return err
}
