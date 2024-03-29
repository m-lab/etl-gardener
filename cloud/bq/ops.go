package bq

import (
	"bytes"
	"context"
	"errors"
	"html/template"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/m-lab/etl-gardener/tracker"
)

var (
	// ErrTableNotFound is returned when trying to load to non-existent tmp tables.
	ErrTableNotFound = errors.New("table not found")
	// ErrDatatypeNotSupported is returned by Query for unsupported datatypes.
	ErrDatatypeNotSupported = errors.New("datatype not supported")

	// ErrNilBigQueryClient is returned when an invalid client is provided.
	ErrNilBigQueryClient = errors.New("nil bigquery client")
	// ErrNilQuery is returned when the query is empty or nil.
	ErrNilQuery = errors.New("bigquery query is empty or nil")
)

// TableOps is used to construct and execute table partition operations.
type TableOps struct {
	client     bqiface.Client
	LoadSource string // The bucket/path to load from.
	Project    string
	Date       string // Name of the partition field
	Job        tracker.Job
	// map key is the single field name, value is fully qualified name
	PartitionKeys map[string]string
	OrderKeys     string
}

// NewTableOps creates a suitable QueryParams for a Job.
// The context is used to create a bigquery client, and should be kept alive while
// the querier is in use.
func NewTableOps(ctx context.Context, job tracker.Job, project string, loadSource string) (*TableOps, error) {
	c, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	return NewTableOpsWithClient(bqClient, job, project, loadSource)
}

// NewTableOpsWithClient creates a suitable QueryParams for a Job.
func NewTableOpsWithClient(client bqiface.Client, job tracker.Job, project string, loadSource string) (*TableOps, error) {
	switch job.Datatype {
	case "switch":
		fallthrough
	case "annotation2":
		fallthrough
	case "hopannotation2":
		fallthrough
	case "pcap":
		fallthrough
	case "scamper1":
		fallthrough
	case "tcpinfo":
		fallthrough
	case "ndt5":
		fallthrough
	case "ndt7":
		return &TableOps{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "date",
			Job:           job,
			PartitionKeys: map[string]string{"id": "id"},
			OrderKeys:     "",
		}, nil

	default:
		return nil, ErrDatatypeNotSupported
	}
}

var queryTemplates = map[string]*template.Template{
	"dedup": dedupTemplate,
}

// makeQuery creates a query from a template.
func (to TableOps) makeQuery(t *template.Template) string {
	out := bytes.NewBuffer(nil)
	err := t.Execute(out, to)
	if err != nil {
		log.Println(err)
	}
	return out.String()
}

// dedupQuery returns the appropriate query in string form.
func dedupQuery(to TableOps) string {
	return to.makeQuery(dedupTemplate)
}

// Dedup initiates a deduplication query, and returns the bqiface.Job.
func (to TableOps) Dedup(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	qs := dedupQuery(to)
	if len(qs) == 0 {
		return nil, ErrNilQuery
	}
	if to.client == nil {
		return nil, ErrNilBigQueryClient
	}
	q := to.client.Query(qs)
	if q == nil {
		return nil, ErrNilQuery
	}
	qc := bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			Q:      qs,
			DryRun: dryRun,
			// Schedule as batch job to avoid quota limits for interactive jobs.
			Priority: bigquery.BatchPriority,
		},
	}
	q.SetQueryConfig(qc)
	return q.Run(ctx)
}

// LoadToTmp loads the "tmp" experiment table from files previously written to GCS by the parsers (or other source).
func (to TableOps) LoadToTmp(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if to.client == nil {
		return nil, ErrNilBigQueryClient
	}

	gcsRef := bigquery.NewGCSReference(to.LoadSource)
	gcsRef.SourceFormat = bigquery.JSON

	dest := to.client.
		Dataset(to.Job.Datasets.Tmp).
		Table(to.Job.Datatype)
	if dest == nil {
		return nil, ErrTableNotFound
	}
	loader := dest.LoaderFrom(gcsRef)
	loadConfig := bqiface.LoadConfig{}
	loadConfig.WriteDisposition = bigquery.WriteAppend
	loadConfig.Dst = dest
	loadConfig.Src = gcsRef
	loader.SetLoadConfig(loadConfig)

	return loader.Run(ctx)
}

// CopyToRaw copies the job "tmp" table partition to the "raw" table partition.
func (to TableOps) CopyToRaw(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if to.client == nil {
		return nil, ErrNilBigQueryClient
	}
	tableName := to.Job.TablePartition()
	src := to.client.Dataset(to.Job.Datasets.Tmp).Table(tableName)
	dest := to.client.Dataset(to.Job.Datasets.Raw).Table(tableName)

	copier := dest.CopierFrom(src)
	config := bqiface.CopyConfig{}
	config.WriteDisposition = bigquery.WriteTruncate
	config.Dst = dest
	config.Srcs = append(config.Srcs, src)
	copier.SetCopyConfig(config)
	return copier.Run(ctx)
}

const tmpTable = "`{{.Project}}.{{.Job.Datasets.Tmp}}.{{.Job.Datatype}}`"
const rawTable = "`{{.Project}}.{{.Job.Datasets.Raw}}.{{.Job.Datatype}}`"

// NOTE: experiment annotations must come from the same raw experiment dataset.
const annoTable = "`{{.Project}}.{{.Job.Datasets.Raw}}.annotation2`"

var dedupTemplate = template.Must(template.New("").Parse(`
#standardSQL
# Delete all duplicate rows based on key and prefered priority ordering.
# This is resource intensive for tcpinfo - 20 slot hours for 12M rows with 250M snapshots,
# roughly proportional to the memory footprint of the table partition.
# The query is very cheap if there are no duplicates.
DELETE
FROM ` + tmpTable + ` AS target
WHERE {{.Date}} = "{{.Job.Date.Format "2006-01-02"}}"
# This identifies all rows that don't match rows to preserve.
AND NOT EXISTS (
  # This creates list of rows to preserve, based on key and priority.
  WITH keep AS (
  SELECT * EXCEPT(row_number) FROM (
    SELECT
      {{range $k, $v := .PartitionKeys}}{{$v}}, {{end}}
	  parser.Time,
      ROW_NUMBER() OVER (
        PARTITION BY {{range $k, $v := .PartitionKeys}}{{$v}}, {{end}}date
        ORDER BY {{.OrderKeys}} parser.Time DESC
      ) row_number
      FROM (
        SELECT * FROM ` + tmpTable + `
        WHERE {{.Date}} = "{{.Job.Date.Format "2006-01-02"}}"
      )
    )
    WHERE row_number = 1
  )
  SELECT * FROM keep
  # This matches against the keep table based on keys.  Sufficient select keys must be
  # used to distinguish the preferred row from the others.
  WHERE
    {{range $k, $v := .PartitionKeys}}target.{{$v}} = keep.{{$k}} AND {{end}}
    target.parser.Time = keep.Time
)`))

// DeleteTmp deletes the tmp table partition.
func (to TableOps) DeleteTmp(ctx context.Context) error {
	if to.client == nil {
		return ErrNilBigQueryClient
	}
	tmp := to.client.Dataset(to.Job.Datasets.Tmp).Table(to.Job.TablePartition())
	log.Println("Deleting", tmp.FullyQualifiedName())
	return tmp.Delete(ctx)
}

// joinTemplate is used to create join queries, based on Job details.
// TODO - explore whether using query parameters would improve readability
// instead of executing this template.  Query params cannot be used for
// table names, but would work for most other variables.
var joinTemplate = template.Must(template.New("").Parse(`
#standardSQL
# Join the ndt7 data with server and client annotation.
WITH {{.Job.Datatype}} AS (
SELECT *
FROM ` + rawTable + `
WHERE {{.Date}} = "{{.Job.Date.Format "2006-01-02"}}"
),

# Need to remove dups?
ann AS (
SELECT *
FROM ` + annoTable + `
WHERE {{.Date}} BETWEEN DATE_SUB("{{.Job.Date.Format "2006-01-02"}}", INTERVAL 1 DAY) AND "{{.Job.Date.Format "2006-01-02"}}"
)

SELECT {{.Job.Datatype}}.id, {{.Job.Datatype}}.date, {{.Job.Datatype}}.parser,
       ann.* EXCEPT(id, date, parser), {{.Job.Datatype}}.* EXCEPT(id, date, parser)
FROM {{.Job.Datatype}} LEFT JOIN ann USING (id)
`))

// Join joins the raw tables into annotated tables.
func (to TableOps) Join(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	qs := to.makeQuery(joinTemplate)

	if len(qs) == 0 {
		return nil, ErrNilQuery
	}
	if to.client == nil {
		return nil, ErrNilBigQueryClient
	}
	q := to.client.Query(qs)
	if q == nil {
		return nil, ErrNilQuery
	}
	// The destintation is a partition in a table based on the job
	// type and date.  Initially, this will only be ndt7.
	dest := to.client.Dataset(to.Job.Datasets.Join).Table(to.Job.TablePartition())
	qc := bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			DryRun: dryRun,
			Q:      qs,
			// We want to replace the whole partition
			WriteDisposition: bigquery.WriteTruncate,
			// Create the table if it doesn't exist
			CreateDisposition: bigquery.CreateIfNeeded,
			// Allow additional fields introduced by the raw tables to be automatically
			// added to the joined, materialized output table.
			SchemaUpdateOptions: []string{"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"},
			// Partitioning spec, in event we have to create the table.
			TimePartitioning: &bigquery.TimePartitioning{
				Field:                  "date",
				RequirePartitionFilter: true,
			},
			// Schedule as batch job to avoid quota limits for interactive jobs.
			Priority: bigquery.BatchPriority,
		},
		Dst: dest,
	}
	q.SetQueryConfig(qc)
	return q.Run(ctx)
}
