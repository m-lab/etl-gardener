package ops

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
		t.Error("keep.uuid")
	}
	if !strings.Contains(q, `"2019-03-04"`) {
		t.Error("date\n" + q)
	}
	if !strings.Contains(q, "ParseInfo.TaskFileName") {
		t.Error("Task filename")
	}
}
