package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(WarningCount)
	prometheus.MustRegister(TasksInFlight)
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
			Name: "gardener_fail_total",
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
			Name: "gardener_warning_total",
			Help: "Number of processing warnings.",
		},
		[]string{"status"},
	)

	// TasksInFlight maintains a count of the number of tasks in flight.
	//
	// Provides metrics:
	//   gardener_tasks_in_flight
	// Example usage:
	// metrics.TasksInFlight.Inc()
	TasksInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gardener_tasks_in_flight",
			Help: "Number of tasks in flight",
		},
	)
)
