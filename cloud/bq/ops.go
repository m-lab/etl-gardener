package bq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/tracker"
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

// ErrDatatypeNotSupported is returned by Query for unsupported datatypes.
var ErrDatatypeNotSupported = errors.New("Datatype not supported")

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
	case "annotation":
		return &TableOps{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "date",
			Job:           job,
			PartitionKeys: map[string]string{"id": "id"},
			OrderKeys:     "",
		}, nil

	case "ndt5":
		return &TableOps{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "DATE(log_time)",
			Job:           job,
			PartitionKeys: map[string]string{"test_id": "test_id"},
			OrderKeys:     "",
		}, nil

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

	case "tcpinfo":
		return &TableOps{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "DATE(TestTime)",
			Job:           job,
			PartitionKeys: map[string]string{"uuid": "uuid", "Timestamp": "FinalSnapshot.Timestamp"},
			// TODO TaskFileName should be ArchiveURL once we update the schema.
			OrderKeys: "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ",
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

// DedupQuery returns the appropriate query in string form.
func dedupQuery(to TableOps) string {
	switch to.Job.Datatype {
	case "ndt5":
		fallthrough
	case "tcpinfo":
		return to.makeQuery(oldDedupTemplate)
	default:
		return to.makeQuery(dedupTemplate)
	}
}

// Dedup initiates a deduplication query, and returns the bqiface.Job.
func (to TableOps) Dedup(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	qs := dedupQuery(to)
	if len(qs) == 0 {
		return nil, dataset.ErrNilQuery
	}
	if to.client == nil {
		return nil, dataset.ErrNilBqClient
	}
	q := to.client.Query(qs)
	if q == nil {
		return nil, dataset.ErrNilQuery
	}
	if dryRun {
		qc := bqiface.QueryConfig{QueryConfig: bigquery.QueryConfig{DryRun: dryRun, Q: qs}}
		q.SetQueryConfig(qc)
	}
	return q.Run(ctx)
}

// LoadToTmp loads the tmp_ exp table from GCS files.
func (to TableOps) LoadToTmp(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if to.client == nil {
		return nil, dataset.ErrNilBqClient
	}

	gcsRef := bigquery.NewGCSReference(to.LoadSource)
	gcsRef.SourceFormat = bigquery.JSON

	dest := to.client.
		Dataset("tmp_" + to.Job.Experiment).
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

// CopyToRaw copies the tmp_ job partition to the raw_ job partition.
func (to TableOps) CopyToRaw(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if to.client == nil {
		return nil, dataset.ErrNilBqClient
	}
	tableName := to.Job.Datatype + "$" + to.Job.Date.Format("20060102")
	src := to.client.Dataset("tmp_" + to.Job.Experiment).Table(tableName)
	dest := to.client.Dataset("raw_" + to.Job.Experiment).Table(tableName)

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

var oldDedupTemplate = template.Must(template.New("").Parse(`
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
	  parseInfo.ParseTime,
      ROW_NUMBER() OVER (
        PARTITION BY {{range $k, $v := .PartitionKeys}}{{$v}}, {{end}} {{.Date}}
        ORDER BY {{.OrderKeys}} ParseInfo.ParseTime DESC
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
    target.ParseInfo.ParseTime = keep.ParseTime
)`))

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
		return dataset.ErrNilBqClient
	}
	// TODO - name should be field in queryer.
	tmp := to.client.Dataset("tmp_" + to.Job.Experiment).Table(
		fmt.Sprintf("%s$%s", to.Job.Datatype, to.Job.Date.Format("20060102")))
	log.Println("Deleting", tmp.FullyQualifiedName())
	return tmp.Delete(ctx)
}
