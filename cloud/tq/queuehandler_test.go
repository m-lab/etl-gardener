package tq_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// NOTE: This test is just slow because it triggers 10 requests to cloud storage.
func TestChannelQueueHandler(t *testing.T) {
	client, counter := cloud.DryRunClient()
	config := cloud.Config{Project: "mlab-testing", Client: client, Options: nil, TestMode: true}
	cqh, err := tq.NewChannelQueueHandler(config, "test-queue", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 19; i < 29; i++ {
		cqh.Sink() <- state.Task{Name: fmt.Sprintf("gs://archive-mlab-testing/ndt/2017/09/%2d/", i)}
	}
	close(cqh.Sink())
	<-cqh.Response()
	// There will be one http request for each GetTaskqueueStatistics() call (10), and one for each task file (76).
	if counter.Count() != 18 {
		log.Println(counter.Count())
		t.Errorf("Count = %d, expected 18", counter.Count())
	}
}
