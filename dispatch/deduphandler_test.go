package dispatch_test

import (
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/dispatch"
)

func TestDedupHandler(t *testing.T) {

	os.Setenv("PROJECT", "mlab-testing")
	os.Setenv("DATASET", "batch")

	src, done, err := dispatch.NewDedupHandler("foobar")

	if err != nil {
		t.Fatal(err)
	}

	src <- "gs://gfr/sidestream/2001/01/01/"
	close(src)
	<-done

}
