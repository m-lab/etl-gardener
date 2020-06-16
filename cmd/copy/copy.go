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

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	exp  = flag.String("exp", "ndt7", "experiment")
	date = flag.String("date", "2020/05/22", "date")
)

// TODO improve test coverage?
func copyFunc(ctx context.Context, j tracker.Job) {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	//delay := time.Since(stateChangeTime).Round(time.Minute)

	var bqJob bqiface.Job
	qp, err := bq.NewQuerier(ctx, j, "mlab-sandbox", "")
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
		msg = fmt.Sprintf("Copy took %s (after %s waiting), %d MB Processed",
			opTime.Round(100*time.Millisecond),
			"xxx", //delay,
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
	j := tracker.NewJob("bucket", "ndt", *exp, d)
	log.Println(j)
	copyFunc(ctx, j)
}
