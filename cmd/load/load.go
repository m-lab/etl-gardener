package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
)

var (
	datatype = flag.String("datatype", "ndt7", "data type")
	date     = flag.String("date", "", "date")
)

var usageText = `
NAME
  load - load from a GCS prefix into tmp_ndt.*

DESCRIPTION
  loads data for a single datatype/date from json files in GCS into mlab-sandbox.tmp_ndt.datatype table.
  Intended primarily for testing and development.

EXAMPLES
  load -datatype=ndt7 -date=2020-03-01
`

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), usageText)
		flag.PrintDefaults()
	}
}

func main() {
	ctx := context.Background()

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	d, err := time.Parse("2006-01-02", *date)
	if err != nil {
		log.Fatal(err)
	}
	j := tracker.NewJob("bucket", "ndt", *datatype, d)
	log.Println(j)

	q, err := bq.NewTableOps(ctx, j, "mlab-sandbox",
		fmt.Sprintf("gs://json-mlab-sandbox/%s/%s/%s",
			j.Experiment, j.Datatype, d.Format("2006/01/02/*")))
	if err != nil {
		log.Fatal(err)
	}

	bqj, err := q.LoadToTmp(ctx, false)
	if err != nil {
		log.Fatal(err)
	}

	status, err := bqj.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(status, status.Statistics.Details)
}
