package metrics

import (
	"testing"

	"github.com/m-lab/go/prometheusx/promtest"
)

func TestLintMetrics(t *testing.T) {
	StartedCount.WithLabelValues("exp", "type", "status"
	CompletedCount.WithLabelValues("exp", "type", "status"
	FailCount.WithLabelValues("exp", "type", "status"
	WarningCount.WithLabelValues("exp", "type", "status")
	StateDate.WithLabelValues("exp", "type", "x")
	StateTimeHistogram.WithLabelValues("exp", "type", "x")
	FilesPerDateHistogram.WithLabelValues("exp", "type", "x")
	BytesPerDateHistogram.WithLabelValues("exp", "type", "x")
	promtest.LintMetrics(nil) // Log warnings only.
}
