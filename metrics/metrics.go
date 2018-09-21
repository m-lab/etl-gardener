package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(WarningCount)
	prometheus.MustRegister(TasksInFlight)
	prometheus.MustRegister(StartedCount)
	prometheus.MustRegister(CompletedCount)
	prometheus.MustRegister(StateTimeSummary)
}

var (
	// StartedCount counts the number of date tasks started.  This does not include
	// restarts.
	//
	// Provides metrics:
	//   gardener_started_total{experiment}
	// Example usage:
	// metrics.StartCount.WithLabelValues("sidestream").Inc()
	StartedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_started_total",
			Help: "Number of date tasks started.",
		},
		[]string{"experiment"},
	)

	// CompletedCount counts the number of date tasks completed.
	//
	// Provides metrics:
	//   gardener_completed_total{experiment}
	// Example usage:
	// metrics.CompletedCount.WithLabelValues("sidestream").Inc()
	CompletedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_completed_total",
			Help: "Number of date tasks completed.",
		},
		[]string{"experiment"},
	)

	// FailCount counts the number of requests that result in a fatal failure.
	// These occur when a request cannot be completed.
	//
	// Provides metrics:
	//   gardener_fail_total{status}
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
	//   gardener_warning_total{status}
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

	// StateTimeSummary measures the time spent in different task states.
	// Provides metrics:
	//    gardener_state_time_summary
	// Example usage:
	//    metrics.StateTimeSummary.WithLabelValues("Queuing").observe(float64)
	StateTimeSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "gardener_state_time_summary",
		Help: "The time spent in each state.",
	}, []string{"state"})
)
