package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(WarningCount)
}

var (
	// FailCount counts the number of requests that result in a fatal failure.
	// These occur when a request cannot be completed.
	//
	// Provides metrics:
	//   gardener_fail_count{status}
	// Example usage:
	// metrics.FailCount.WithLabelValues("BadTableName").Inc()
	FailCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_fail_count",
			Help: "Number of processing failures.",
		},
		[]string{"status"},
	)

	// WarningCount counts all warnings encountered during processing a request.
	//
	// Provides metrics:
	//   gardener_warning_count{status}
	// Example usage:
	// metrics.WarningCount.WithLabelValues("funny xyz").Inc()
	WarningCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_warning_count",
			Help: "Number of processing warnings.",
		},
		[]string{"status"},
	)
)
