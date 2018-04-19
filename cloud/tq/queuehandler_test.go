package tq_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestChannelQueueHandler(t *testing.T) {
	client, counter := tq.DryRunQueuerClient()
	cqh, err := tq.NewChannelQueueHandler(client, "mlab-testing", "test-queue", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 19; i < 29; i++ {
		cqh.Sink() <- state.Task{Name: fmt.Sprintf("gs://archive-mlab-test/ndt/2017/09/%2d/", i)}
	}
	close(cqh.Sink())
	<-cqh.Response()
	// There will be one http request for each IsEmpty() call (10), and one for each task file (76).
	if counter.Count() != 87 {
		log.Println(counter.Count())
		t.Error("Count != 87")
	}
}
