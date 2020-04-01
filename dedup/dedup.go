// Package dedup provides functions for deduplicating bigquery table partitions.
package dedup

import (
	"bytes"
	"context"
	"html/template"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/m-lab/go/dataset"

	"github.com/m-lab/etl-gardener/tracker"
)

// TODO remove _tmp when we are ready to transition.
// TODO update to use _tmp once tables are copied.
const table = "`{{.Project}}.{{.Job.Experiment}}_raw.{{.Job.Datatype}}`"

var dedupTemplate = template.Must(template.New("").Parse(`
#standardSQL
# Delete all duplicate rows based on key and prefered priority ordering.
# This is resource intensive for tcpinfo - 20 slot hours for 12M rows with 250M snapshots,
# roughly proportional to the memory footprint of the table partition.
# The query is very cheap if there are no duplicates.
DELETE
FROM ` + table + ` AS target
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
          SELECT * FROM ` + table + `
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

func (params QueryParams) String() string {
	out := bytes.NewBuffer(nil)
	err := dedupTemplate.Execute(out, params)
	if err != nil {
		log.Println(err)
	}
	return out.String()
}

func TCPInfoQuery(job tracker.Job, project string) QueryParams {
	return QueryParams{
		Project:   project,
		TestTime:  "TestTime",
		Job:       job,
		Partition: map[string]string{"uuid": "uuid", "Timestamp": "FinalSnapshot.Timestamp"},
		Order:     "ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC",
		Select:    map[string]string{"ParseTime": "ParseInfo.ParseTime"},
	}
}

// Dedup executes a query that deletes duplicates from the destination table.
// It derives the table name from the dsExt project and the job fields.
// TODO add cost accounting to status page?
// TODO inject fake bqclient for testing?
func (params QueryParams) Dedup(ctx context.Context) (bqiface.Job, error) {
	c, err := bigquery.NewClient(ctx, params.Project)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	q := bqClient.Query(params.String())
	if q == nil {
		return nil, dataset.ErrNilQuery
	}
	return q.Run(ctx)
}
