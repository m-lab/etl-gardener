package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(ErrorCount)
	prometheus.MustRegister(WarningCount)
}

var (
	// ErrorCount counts non-fatal errors.
	//
	// Provides metrics:
	//   etl_error_count{status}
	// Example usage:
	// metrics.ErrorCount.WithLabelValues("ok").Inc()
	ErrorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_error_count",
			Help: "Number of non-fatal errors.",
		},
		[]string{"status"},
	)

	// WarningCount counts the number of files processed with each status.
	//
	// Provides metrics:
	//   etl_fail_count{status}
	// Example usage:
	// metrics.WarningCount.WithLabelValues("funny xyz").Inc()
	WarningCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_warning_count",
			Help: "Number of processing warnings.",
		},
		[]string{"status"},
	)

	// FailCount counts the number of files processed with each status.
	//
	// Provides metrics:
	//   etl_fail_count{status}
	// Example usage:
	// metrics.FailCount.WithLabelValues("BadTableName").Inc()
	FailCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etl_fail_count",
			Help: "Number of processing failures.",
		},
		[]string{"status"},
	)
)
