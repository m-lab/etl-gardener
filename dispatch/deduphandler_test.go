package dispatch_test

import (
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/dispatch"
)

func TestDedupHandler(t *testing.T) {

	os.Setenv("PROJECT", "mlab-testing")
	os.Setenv("DATASET", "batch")

	dedup := dispatch.NewDedupHandler("foobar")

	dedup.Sink() <- "gs://gfr/sidestream/2001/01/01/"
	close(dedup.Sink())
	<-dedup.Response()

}
