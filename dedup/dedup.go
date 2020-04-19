// Package dedup provides functions for deduplicating bigquery table partitions.
package dedup

// TODO consider using query parameters to simplify the template.

import (
	"bytes"
	"context"
	"errors"
	"html/template"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/tracker"
)

// QueryParams is used to construct a dedup query.
type QueryParams struct {
	Project  string
	TestTime string // Name of the partition field
	Job      tracker.Job
	// map key is the single field name, value is fully qualified name
	Partition map[string]string
	Order     string
	Select    map[string]string // Derived from Order.
}

// ErrDatatypeNotSupported is returned by Query for unsupported datatypes.
var ErrDatatypeNotSupported = errors.New("Datatype not supported")

// NewQueryParams creates a suitable QueryParams for a Job.
func NewQueryParams(job tracker.Job, project string) (QueryParams, error) {
	switch job.Datatype {
	case "annotation":
		return QueryParams{
			Project:   project,
			TestTime:  "TestTime",
			Job:       job,
			Partition: map[string]string{"UUID": "UUID"},
			Order:     "ParseInfo.ParseTime DESC",
			Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
		}, nil

	case "ndt5":
		return QueryParams{
			Project:   project,
			TestTime:  "log_time",
			Job:       job,
			Partition: map[string]string{"test_id": "test_id"},
			Order:     "ParseInfo.ParseTime DESC",
			Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
		}, nil

	case "ndt7":
		return QueryParams{
			Project:   project,
			TestTime:  "TestTime",
			Job:       job,
			Partition: map[string]string{"UUID": "a.UUID"},
			Order:     "ParseInfo.ParseTime DESC",
			Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
		}, nil

	case "tcpinfo":
		return QueryParams{
			Project:   project,
			TestTime:  "TestTime",
			Job:       job,
			Partition: map[string]string{"uuid": "uuid", "Timestamp": "FinalSnapshot.Timestamp"},
			Order:     "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC",
			Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
		}, nil
	default:
		return QueryParams{}, ErrDatatypeNotSupported
	}
}

var queryTemplates = map[string]*template.Template{
	"dedup":    dedupTemplate,
	"preserve": preserveTemplate,
	"cleanup":  cleanupTemplate,
}

// MakeQuery creates a query from a template.
func (params QueryParams) makeQuery(t *template.Template) string {
	out := bytes.NewBuffer(nil)
	err := t.Execute(out, params)
	if err != nil {
		log.Println(err)
	}
	return out.String()
}

// QueryString returns the appropriate query in string form.
func (params QueryParams) QueryString(key string) string {
	t, ok := queryTemplates[key]
	if !ok {
		return ""
	}
	return params.makeQuery(t)
}

// Run executes a query constructed from a template.  It returns the bqiface.Job.
func (params QueryParams) Run(ctx context.Context, key string, dryRun bool) (bqiface.Job, error) {
	c, err := bigquery.NewClient(ctx, params.Project)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	qs := params.QueryString(key)
	if len(qs) == 0 {
		return nil, dataset.ErrNilQuery
	}
	q := bqClient.Query(qs)
	if q == nil {
		return nil, dataset.ErrNilQuery
	}
	if dryRun {
		qc := bqiface.QueryConfig{QueryConfig: bigquery.QueryConfig{DryRun: dryRun, Q: qs}}
		q.SetQueryConfig(qc)
	}
	return q.Run(ctx)
}

// Copy copies the tmp_ job partition to the raw_ job partition.
func (params QueryParams) Copy(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	c, err := bigquery.NewClient(ctx, params.Project)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	src := bqClient.Dataset("tmp_" + params.Job.Experiment).Table(params.Job.Datatype)
	dest := bqClient.Dataset("raw_" + params.Job.Experiment).Table(params.Job.Datatype)

	copier := dest.CopierFrom(src)
	config := bqiface.CopyConfig{}
	config.WriteDisposition = bigquery.WriteTruncate
	config.Dst = dest
	config.Srcs = append(config.Srcs, src)
	copier.SetCopyConfig(config)
	return copier.Run(ctx)
}

// TODO get the tmp_ and raw_ from the job Target?
const tmpTable = "`{{.Project}}.tmp_{{.Job.Experiment}}.{{.Job.Datatype}}`"
const rawTable = "`{{.Project}}.raw_{{.Job.Experiment}}.{{.Job.Datatype}}`"

var dedupTemplate = template.Must(template.New("").Parse(`
#standardSQL
# Delete all duplicate rows based on key and prefered priority ordering.
# This is resource intensive for tcpinfo - 20 slot hours for 12M rows with 250M snapshots,
# roughly proportional to the memory footprint of the table partition.
# The query is very cheap if there are no duplicates.
DELETE
FROM ` + tmpTable + ` AS target
WHERE Date({{.TestTime}}) = "{{.Job.Date.Format "2006-01-02"}}"
# This identifies all rows that don't match rows to preserve.
AND NOT EXISTS (
  # This creates list of rows to preserve, based on key and priority.
  WITH keep AS (
  SELECT * EXCEPT(row_number) FROM (
    SELECT
      {{range $k, $v := .Partition}}{{$v}}, {{end}}
      {{range $k, $v := .Select}}{{$v}}, {{end}}
      ROW_NUMBER() OVER (
        PARTITION BY {{range $k, $v := .Partition}}{{$v}}, {{end}}TRUE
        ORDER BY {{.Order}}
      ) row_number
      FROM (
        SELECT * FROM ` + tmpTable + `
        WHERE Date({{.TestTime}}) = "{{.Job.Date.Format "2006-01-02"}}"
      )
    )
    WHERE row_number = 1
  )
  SELECT * FROM keep
  # This matches against the keep table based on keys.  Sufficient select keys must be
  # used to distinguish the preferred row from the others.
  WHERE
    {{range $k, $v := .Partition}}target.{{$v}} = keep.{{$k}} AND {{end}}
    {{range $k, $v := .Select}}target.{{$v}} = keep.{{$k}} AND {{end}}TRUE
)`))

// Dedup executes a query that deletes duplicates from the destination table.
func (params QueryParams) Dedup(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	return params.Run(ctx, "dedup", dryRun)
}

var preserveTemplate = template.Must(template.New("").Parse(`
UNIMPLEMENTED
#standardSQL
# Delete all rows in a partition.
DELETE
FROM ` + tmpTable + ` AS target
WHERE Date({{.TestTime}}) = "{{.Job.Date.Format "2006-01-02"}}"
`))

// Preserve executes a query that finds rows in raw_ table that are missing from
// the tmp_ table, and copies them.  This can be used instead of sanity check, since
// it prevents the loss of rows.
// HOWEVER: We still need to ensure that the tmp_ partition hasn't been deleted
// before copying it to the raw_ table.
func (params QueryParams) Preserve(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	return params.Run(ctx, "preserve", dryRun)
}

var cleanupTemplate = template.Must(template.New("").Parse(`
#standardSQL
# Delete all rows in a partition.
DELETE
FROM ` + tmpTable + ` AS target
WHERE Date({{.TestTime}}) = "{{.Job.Date.Format "2006-01-02"}}"
`))

// Cleanup executes a query that deletes the entire partition
// from the tmp table.
func (params QueryParams) Cleanup(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	return params.Run(ctx, "cleanup", dryRun)
}
