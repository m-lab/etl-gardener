package dedup

import (
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestTemplate(t *testing.T) {
	job := tracker.NewJob("bucket", "exp", "type", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q := tcpinfoQuery(job, "mlab-sandbox")
	if !strings.Contains(q, "uuid") {
		t.Error("query should contain keep.uuid")
	}
	if !strings.Contains(q, `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04"`)
	}
	if !strings.Contains(q, "ParseInfo.TaskFileName") {
		t.Error("query should contain ParseInfo.TaskFileName")
	}
}
