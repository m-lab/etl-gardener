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
	prometheus.MustRegister(StateTimeHistogram)
	prometheus.MustRegister(StateDate)
	prometheus.MustRegister(FilesPerDateHistogram)
	prometheus.MustRegister(BytesPerDateHistogram)
}

var (
	// StartedCount counts the number of date tasks started.  This does not include
	// restarts.
	//
	// Provides metrics:
	//   gardener_started_total{experiment}
	// Example usage:
	// metrics.StartedCount.WithLabelValues("sidestream").Inc()
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
	// metrics.TasksInFlight.Add(1)
	TasksInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "gardener_tasks_in_flight",
			Help: "Number of tasks in flight",
		},
	)

	// StateDate identifies the date of the most recent update to each state.
	//
	// Provides metrics:
	//   gardener_state_date
	// Example usage:
	// metrics.StateDate.WithLabelValues(StateNames[t.State]).Observe(time.Now())
	StateDate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gardener_state_date",
			Help: "Most recent date for each state.",
		},
		[]string{"state"},
	)

	// StateTimeSummary measures the time spent in different task states.
	// DEPRECATED - remove soon.
	// Provides metrics:
	//    gardener_state_time_summary
	// Example usage:
	//    metrics.StateTimeSummary.WithLabelValues("Queuing").observe(float64)
	StateTimeSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "gardener_state_time_summary",
		Help:       "The time spent in each state.",
		Objectives: map[float64]float64{0.01: 0.001, 0.1: 0.01, 0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"state"},
	)

	// StateTimeHistogram tracks the time spent in each state.  Not necessary to label data type, as
	// we currently have separate gardener deployments for each type.
	// Usage example:
	//   metrics.StateTimeHistogram.WithLabelValues(
	//           StateName[state]).Observe(time.Since(start).Seconds())
	StateTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "gardener_state_time_histogram",
			Help: "time-in-state distributions.",
			// These values range from seconds to hours.
			Buckets: []float64{
				0.1, 0.3, 1, 3, 10, 30,
				100, 300, 1000, 1800, 3600, 2 * 3600, 4 * 3600, 8 * 3600, 12 * 3600,
			},
		},
		[]string{"state"})

	// FilesPerDateHistogram provides a histogram of files per date submitted to pipeline.
	//
	// Provides metrics:
	//   gardener_files_bucket{year="...", le="..."}
	//   ...
	//   gardener_files_sum{year="...", le="..."}
	//   gardener_files_count{year="...", le="..."}
	// Usage example:
	//   metrics.FilesPerDateHistogram.WithLabelValues(
	//           "2011").Observe(files)
	FilesPerDateHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "gardener_files",
			Help: "Histogram of number of files submitted per date",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9,
				10, 12, 14, 17, 20, 24, 28, 32, 38, 44, 50, 60, 70, 80, 90,
				100, 120, 140, 170, 200, 240, 280, 320, 380, 440, 500, 600, 700, 800, 900,
				1000, 1200, 1400, 1700, 2000, 2400, 2800, 3200, 3800, 4400, 5000, 6000, 7000, 8000, 9000,
				10000, 12000, 14000, 17000, 20000, 24000, 28000, 32000, 38000, 44000, 50000, 60000, 70000, 80000, 90000,
				100000, 120000, 140000, 170000, 200000, 240000, 280000, 320000, 380000, 440000, 500000, 600000, 700000, 800000, 900000,
			},
		},
		[]string{"year"},
	)

	// BytesPerDateHistogram provides a histogram of bytes per date submitted to pipeline
	//
	// Provides metrics:
	//   gardener_bytes_bucket{year="...", le="..."}
	//   ...
	//   gardener_bytes_sum{year="...", le="..."}
	//   gardener_bytes_count{year="...", le="..."}
	// Usage example:
	//   metrics.BytesPerDateHistogram.WithLabelValues(
	//           "2011").Observe(bytes)
	BytesPerDateHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "gardener_bytes",
			Help: "Histogram of number of bytes submitted per date",
			Buckets: []float64{
				100000, 140000, 200000, 280000, 400000, 560000, 800000,
				1000000, 1400000, 2000000, 2800000, 4000000, 5600000, 8000000,
				10000000, 14000000, 20000000, 28000000, 40000000, 56000000, 80000000,
				100000000, 140000000, 200000000, 280000000, 400000000, 560000000, 800000000,
				1000000000, 1400000000, 2000000000, 2800000000, 4000000000, 5600000000, 8000000000,
				10000000000, 14000000000, 20000000000, 28000000000, 40000000000, 56000000000, 80000000000,
			},
		},
		[]string{"year"},
	)
)
