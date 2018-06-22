package dispatch_test

import (
	"log"
	"strings"
	"testing"

	"github.com/m-lab/etl-gardener/api"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/dispatch"
	"github.com/m-lab/etl-gardener/state"
	"google.golang.org/api/option"
)

// This just asserts that DedupHandler satisfies the Downstream interface.
func assertTaskPipe() {
	func(ds api.TaskPipe) {}(&dispatch.DedupHandler{})
}

// This is much too whitebox.  Can we find better abstractions to improve testing?
func TestDedupHandler(t *testing.T) {
	// Use a fake client so we intercept all the http ops.
	client, counter := cloud.DryRunClient()

	config := cloud.Config{Project: "mlab-testing", Client: client,
		Options: []option.ClientOption{option.WithHTTPClient(client)}, TestMode: true}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "mlab-testing", BQDataset: "batch"}

	dedup := dispatch.NewDedupHandler(bqConfig)

	// TODO - also test with inconsistent state.
	dedup.Sink() <- state.Task{Name: "gs://gfr/sidestream/2001/01/01/", State: state.Stabilizing}
	close(dedup.Sink())
	<-dedup.Response()

	// Should be three HTTP calls altogether.
	// TODO - really should be 5, to do the table copy and intermediate table delete.
	if counter.Count() != 3 {
		t.Errorf("Count was %d instead of 3", counter.Count())
	}

	reqs := counter.Requests()
	// First request should be table metadata call.
	if counter.Count() > 0 && !strings.Contains(reqs[0].URL.String(), "mlab-testing/datasets/batch/tables/sidestream_20010101") {
		log.Printf("%+v\n", *reqs[0])
		t.Error("Did not see expected request")
	}
	// Second request should be a deduplication request.
	if counter.Count() > 1 && reqs[1].URL.String() != "https://www.googleapis.com/bigquery/v2/projects/mlab-testing/jobs?alt=json" {
		log.Printf("%+v\n", *reqs[1])
		t.Error("Did not see expected request")
	}
	// Third request should delete the source table.
	if counter.Count() > 2 && reqs[2].Method != "DELETE" {
		log.Printf("%+v\n", *reqs[2])
		t.Error("Did not see expected request")
	}
}
