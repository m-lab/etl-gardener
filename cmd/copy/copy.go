package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
)

var (
	dataType = flag.String("datatype", "ndt7", "datatype")
	date     = flag.String("date", "", "partition date")
)

var usageText = `
NAME
  copy - copy partition from mlab-sandbox.tmp_ndt to raw_ndt

DESCRIPTION
  copy copies a single partition from a table in mlab-sandbox.tmp_ndt to a table in mlab-sandbox.raw_ndt
  It is intended for manual testing of the bq.TableOps.CopyTmpToRaw, and has no other practical purpose.

EXAMPLES
  copy -datatype=ndt7 -date=2020-03-01
`

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), usageText)
		flag.PrintDefaults()
	}
}

func copyFunc(ctx context.Context, j tracker.Job) {
	var bqJob bqiface.Job
	qp, err := bq.NewTableOps(ctx, j, "mlab-sandbox", "")
	if err != nil {
		log.Println(err)
		return
	}
	bqJob, err = qp.CopyToRaw(ctx, false)
	if err != nil {
		log.Println(err)
		return
	}
	status, err := bqJob.Wait(ctx)
	if err != nil {
		log.Println(err)
		return
	}

	var msg string
	stats := status.Statistics
	if stats != nil {
		opTime := stats.EndTime.Sub(stats.StartTime)
		msg = fmt.Sprintf("Copy took %s, %d MB Processed",
			opTime.Round(100*time.Millisecond),
			stats.TotalBytesProcessed/1000000)
		log.Println(msg)
	}
	return
}

func main() {
	ctx := context.Background()

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	d, err := time.Parse("2006-01-02", *date)
	if err != nil {
		log.Fatal(err)
	}
	j := tracker.NewJob("unused-bucket", "unused-experiment", *dataType, d)
	log.Println(j)
	copyFunc(ctx, j)
}
