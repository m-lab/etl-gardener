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
	"google.golang.org/api/iterator"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/tracker"
)

// OpsHandler provides the interface for running bigquery operations.
type OpsHandler interface {
	DedupQuery() string

	LoadToTmp(ctx context.Context, dryRun bool) (bqiface.Job, error)
	Dedup(ctx context.Context, dryRun bool) (bqiface.Job, error)
	CopyToRaw(ctx context.Context, dryRun bool) (bqiface.Job, error)
	DeleteTmp(ctx context.Context) error
}

// queryer is used to construct a dedup query.
type queryer struct {
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

// NewQuerier creates a suitable OpsHandler for a Job.
// The context is used to create a bigquery client, and should be kept alive while
// the querier is in use.
func NewQuerier(ctx context.Context, job tracker.Job, project string, loadSource string) (OpsHandler, error) {
	c, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	return NewQuerierWithClient(bqClient, job, project, loadSource)
}

// NewQuerierWithClient creates a suitable QueryParams for a Job.
func NewQuerierWithClient(client bqiface.Client, job tracker.Job, project string, loadSource string) (OpsHandler, error) {
	switch job.Datatype {
	case "annotation":
		return &queryer{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "date",
			Job:           job,
			PartitionKeys: map[string]string{"id": "id"},
			OrderKeys:     "",
		}, nil

	case "ndt7":
		return &queryer{
			client:        client,
			LoadSource:    loadSource,
			Project:       project,
			Date:          "date",
			Job:           job,
			PartitionKeys: map[string]string{"id": "id"},
			OrderKeys:     "",
		}, nil

		// TODO: enable tcpinfo again once it supports standard columns.
	/*case "tcpinfo":
	return &queryer{
		client:    client,
		Project:   project,
		Date:      "DATE(TestTime)",
		Job:       job,
		PartitionKeys: map[string]string{"uuid": "uuid", "Timestamp": "FinalSnapshot.Timestamp"},
		// TODO TaskFileName should be ArchiveURL once we update the schema.
		OrderKeys: "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ",
	}, nil
	*/
	default:
		return nil, ErrDatatypeNotSupported
	}
}

var queryTemplates = map[string]*template.Template{
	"dedup": dedupTemplate,
}

// MakeQuery creates a query from a template.
func (params queryer) makeQuery(t *template.Template) string {
	out := bytes.NewBuffer(nil)
	err := t.Execute(out, params)
	if err != nil {
		log.Println(err)
	}
	return out.String()
}

// DedupQuery returns the appropriate query in string form.
func (params queryer) DedupQuery() string {
	return params.makeQuery(dedupTemplate)
}

// Run executes a query constructed from a template.  It returns the bqiface.Job.
func (params queryer) Dedup(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	qs := params.DedupQuery()
	if len(qs) == 0 {
		return nil, dataset.ErrNilQuery
	}
	if params.client == nil {
		return nil, dataset.ErrNilBqClient
	}
	q := params.client.Query(qs)
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
func (params queryer) LoadToTmp(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if params.client == nil {
		return nil, dataset.ErrNilBqClient
	}

	gcsRef := bigquery.NewGCSReference(params.LoadSource)
	gcsRef.SourceFormat = bigquery.JSON

	dest := params.client.
		Dataset("tmp_" + params.Job.Experiment).
		Table(params.Job.Datatype)
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
func (params queryer) CopyToRaw(ctx context.Context, dryRun bool) (bqiface.Job, error) {
	if dryRun {
		return nil, errors.New("dryrun not implemented")
	}
	if params.client == nil {
		return nil, dataset.ErrNilBqClient
	}
	// HACK - for now, we use a standard bq client, because using
	// bqiface doesn't seem to actually work.  However, this may
	// be due to incorrect observations caused by streaming buffer delay,
	// and should be re-evaluated.
	client, err := bigquery.NewClient(ctx, params.client.Dataset("tmp_"+params.Job.Experiment).ProjectID())
	if err != nil {
		return nil, err
	}

	// TODO - names should be fields in queryer.
	src := client.Dataset("tmp_" + params.Job.Experiment).
		Table(params.Job.Datatype + "$" + params.Job.Date.Format("20060102"))
	dest := client.Dataset("raw_" + params.Job.Experiment).
		Table(params.Job.Datatype + "$" + params.Job.Date.Format("20060102"))

	if m, err := src.Metadata(ctx); err == nil {
		log.Printf("Source %s: %+v\n", src.FullyQualifiedName(), m)
	}
	if m, err := dest.Metadata(ctx); err == nil {
		log.Printf("Dest %s: %+v\n", dest.FullyQualifiedName(), m)
	}

	copier := dest.CopierFrom(src)
	copier.CopyConfig.WriteDisposition = bigquery.WriteTruncate
	//log.Printf("%+v\n%+v\n%+v\n", config.Srcs[0], config.Dst, *(*bqiface.Copier)(copier))

	j, err := copier.Run(ctx)

	return &xJob{j: j}, err
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
func (params queryer) DeleteTmp(ctx context.Context) error {
	if params.client == nil {
		return dataset.ErrNilBqClient
	}
	// TODO - name should be field in queryer.
	tmp := params.client.Dataset("tmp_" + params.Job.Experiment).Table(
		fmt.Sprintf("%s$%s", params.Job.Datatype, params.Job.Date.Format("20060102")))
	log.Println("Deleting", tmp.FullyQualifiedName())
	return tmp.Delete(ctx)
}

// This is used to allow using bigquery.Copier as a bqiface.Copier.  YUCK.
type xRowIterator struct {
	i *bigquery.RowIterator
	bqiface.RowIterator
}

func (i *xRowIterator) SetStartIndex(s uint64) {
	i.i.StartIndex = s
}
func (i *xRowIterator) Schema() bigquery.Schema {
	return i.i.Schema
}
func (i *xRowIterator) TotalRows() uint64 {
	return i.i.TotalRows
}
func (i *xRowIterator) Next(p interface{}) error {
	return i.i.Next(p)
}
func (i *xRowIterator) PageInfo() *iterator.PageInfo {
	return i.i.PageInfo()
}

func assertRowIterator() {
	func(bqiface.RowIterator) {}(&xRowIterator{})
}

type xJob struct {
	j *bigquery.Job
	bqiface.Job
}

func (x *xJob) ID() string {
	return x.j.ID()
}
func (x *xJob) Location() string {
	return x.j.Location()
}
func (x *xJob) Config() (bigquery.JobConfig, error) {
	return x.j.Config()
}
func (x *xJob) Status(ctx context.Context) (*bigquery.JobStatus, error) {
	return x.j.Status(ctx)
}
func (x *xJob) LastStatus() *bigquery.JobStatus {
	return x.j.LastStatus()
}
func (x *xJob) Cancel(ctx context.Context) error {
	return x.j.Cancel(ctx)
}
func (x *xJob) Wait(ctx context.Context) (*bigquery.JobStatus, error) {
	return x.j.Wait(ctx)
}
func (x *xJob) Read(ctx context.Context) (bqiface.RowIterator, error) {
	i, err := x.j.Read(ctx)
	return &xRowIterator{i: i}, err
}
