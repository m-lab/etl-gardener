package metrics

import (
	"testing"

	"github.com/m-lab/go/prometheusx/promtest"
)

func TestLintMetrics(t *testing.T) {
	StartedCount.WithLabelValues("exp", "type")
	CompletedCount.WithLabelValues("exp", "type")
	FailCount.WithLabelValues("exp", "type", "status")
	WarningCount.WithLabelValues("exp", "type", "status")
	JobsTotal.WithLabelValues("exp", "type", "false", "status")
	ConfigDatatypes.WithLabelValues("exp", "type")
	StateDate.WithLabelValues("exp", "type", "x")
	StateTimeHistogram.WithLabelValues("exp", "type", "x")
	FilesPerDateHistogram.WithLabelValues("exp", "type", "x")
	BytesPerDateHistogram.WithLabelValues("exp", "type", "x")
	promtest.LintMetrics(nil) // Log warnings only.
}
