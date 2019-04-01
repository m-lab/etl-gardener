package metrics

import (
	"testing"

	"github.com/m-lab/go/prometheusx/promtest"
)

func TestLintMetrics(t *testing.T) {
	StartedCount.WithLabelValues("x")
	CompletedCount.WithLabelValues("x")
	FailCount.WithLabelValues("x")
	WarningCount.WithLabelValues("x")
	StateDate.WithLabelValues("x")
	StateTimeSummary.WithLabelValues("x")
	StateTimeHistogram.WithLabelValues("x")
	FilesPerDateHistogram.WithLabelValues("x")
	BytesPerDateHistogram.WithLabelValues("x")
	promtest.LintMetrics(t)
}
