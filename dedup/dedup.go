package ops

import (
	"bytes"
	"html/template"

	"github.com/m-lab/etl-gardener/tracker"
)

// NOTE: This should probably be in a separate package, and ops should ONLY be
// state machine related stuff, not specific actions.  (actions.go should move too)

const table = "`{{.Project}}.{{.Job.Experiment}}_raw.{{.Job.Datatype}}`"

var dedupTemplate = template.Must(template.New("").Parse(`
#standardSQL
DELETE
FROM ` + table + ` target
  WHERE Date(TestTime) = "{{.Job.Date.Format "2006-01-02"}}"
  AND NOT EXISTS (
    WITH keep AS (
    SELECT * EXCEPT(row_number) FROM (
      SELECT {{.Key}}, ParseInfo.ParseTime
        ROW_NUMBER() OVER (PARTITION BY {{.Key}} ORDER BY {{.Order}}) row_number
        FROM (
          SELECT * FROM ` + table + `
          WHERE Date(TestTime) = "{{.Job.Date.Format "2006-01-02"}}"
        )
      )
      WHERE row_number = 1
    )
    SELECT * FROM keep
    WHERE target.{{.Key}} = keep.{{.Key}} AND target.ParseInfo.ParseTime = keep.ParseTime
  )`))

func makeQuery(job tracker.Job, project, key, order string) string {
	b := bytes.NewBuffer(nil)

	all := struct {
		Project string
		Job     tracker.Job
		Key     string
		Order   string
	}{project, job, key, order}

	dedupTemplate.Execute(b, all)
	return b.String()
}

func tcpinfoQuery(job tracker.Job, project string) string {
	return makeQuery(
		job, project,
		"uuid",
		"ARRAY_LENGTH(Snapshots) DESC, ParseInfo.TaskFileName, ParseInfo.ParseTime DESC",
	)
}

/*
// Dedup executes a query that dedups and writes to destination partition.
// `src` is relative to the project:dataset of dsExt.
// `destTable` specifies the table to write to, typically created with
//   dsExt.BqClient.DatasetInProject(...).Table(...)
//
// NOTE: If destination table is partitioned, destTable MUST include the partition
// suffix to avoid accidentally overwriting the entire table.
// TODO - move these functions to go/bqext package
func Dedup(ctx context.Context, dsExt *dataset.Dataset, pdt bqx.PDT) (bqiface.Job, error) {
	var queryString string
	switch {
	case strings.HasPrefix(destTable.TableID(), "tcpinfo"):
		queryString = template.Execute(dedupTemplateTCPInfo, pdt)
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
}*/
