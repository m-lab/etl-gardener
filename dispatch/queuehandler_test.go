package dispatch_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/dispatch"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestChannelQueueHandler(t *testing.T) {
	client, counter := tq.DryRunQueuerClient()
	c, d, err := dispatch.NewChannelQueueHandler(client, "mlab-testing", "test-queue")
	if err != nil {
		t.Fatal(err)
	}
	for i := 19; i < 29; i++ {
		c <- fmt.Sprintf("gs://archive-mlab-test/ndt/2017/09/%2d/", i)
	}
	close(c)
	<-d
	// There will be one http request for each IsEmpty() call, and one for each task file.
	if counter.Count() != 86 {
		log.Println(counter.Count())
		t.Error("Count != 76")
	}
}
