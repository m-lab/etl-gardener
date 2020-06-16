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

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	exp  = flag.String("exp", "ndt7", "experiment")
	date = flag.String("date", "2020/05/22", "date")
)

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

	q, err := bq.NewQuerier(ctx, j, "mlab-sandbox",
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
