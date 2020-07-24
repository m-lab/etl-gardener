package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// StartedCount counts the number of date tasks started.  This does not include
	// restarts.
	//
	// Provides metrics:
	//   gardener_started_total{experiment}
	// Example usage:
	// metrics.StartedCount.WithLabelValues(exp, dt).Inc()
	StartedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_started_total",
			Help: "Number of date tasks started.",
		},
		[]string{"experiment", "datatype"},
	)

	// CompletedCount counts the number of date tasks completed.
	//
	// Provides metrics:
	//   gardener_completed_total{experiment}
	// Example usage:
	// metrics.CompletedCount.WithLabelValues(exp, dt).Inc()
	CompletedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_completed_total",
			Help: "Number of date tasks completed.",
		},
		[]string{"experiment", "datatype"},
	)

	// FailCount counts the number of requests that result in a fatal failure.
	// These occur when a request cannot be completed.
	//
	// Provides metrics:
	//   gardener_fail_total{status}
	// Example usage:
	// metrics.FailCount.WithLabelValues(exp, dt, "BadTableName").Inc()
	FailCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_fail_total",
			Help: "Number of processing failures.",
		},
		[]string{"experiment", "datatype", "status"}, // TODO Change to failure
	)

	// WarningCount counts all warnings encountered during processing a request.
	//
	// Provides metrics:
	//   gardener_warning_total{status}
	// Example usage:
	// metrics.WarningCount.WithLabelValues(exp, dt, "funny xyz").Inc()
	WarningCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gardener_warning_total",
			Help: "Number of processing warnings.",
		},
		[]string{"experiment", "datatype", "status"}, // TODO change to warning
	)

	// TasksInFlight maintains a count of the number of tasks in flight.
	// TODO consider deprecating this and using Started - Completed.
	//
	// Provides metrics:
	//   gardener_tasks_in_flight
	// Example usage:
	// metrics.TasksInFlight.WithLabelValues(exp, dt).Inc
	TasksInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			// TODO - should call this JobsInFlight (gardener_jobs_in_flight), to avoid confusion with Tasks in parser.
			Name: "gardener_tasks_in_flight",
			Help: "Number of tasks in flight",
		},
		[]string{"experiment", "datatype", "state"},
	)

	// StateDate identifies the date of the most recent update to each state.
	//
	// Provides metrics:
	//   gardener_state_date
	// Example usage:
	// metrics.StateDate.WithLabelValues(exp, dt, StateNames[t.State]).Observe(time.Now())
	StateDate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gardener_state_date",
			Help: "Most recent date for each state.",
		},
		[]string{"experiment", "datatype", "state"},
	)

	// StateTimeHistogram tracks the time spent in each state.  Not necessary to label data type, as
	// we currently have separate gardener deployments for each type.
	// Usage example:
	//   metrics.StateTimeHistogram.WithLabelValues(
	//           exp, dt, StateName[state]).Observe(time.Since(start).Seconds())
	StateTimeHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "gardener_state_time_histogram",
			Help: "time-in-state distributions.",
			// These values range from seconds to hours.
			Buckets: []float64{
				0.1, 0.3, 1, 3, 10, 30,
				100, 300, 1000, 1800, 3600, 2 * 3600, 4 * 3600, 8 * 3600, 12 * 3600,
			},
		},
		[]string{"experiment", "datatype", "state"})

	// FilesPerDateHistogram provides a histogram of files per date submitted to pipeline.
	//
	// Provides metrics:
	//   gardener_files_bucket{year="...", le="..."}
	//   ...
	//   gardener_files_sum{year="...", le="..."}
	//   gardener_files_count{year="...", le="..."}
	// Usage example:
	//   metrics.FilesPerDateHistogram.WithLabelValues(
	//           "ndt", "ndt5", "2011").Observe(files)
	FilesPerDateHistogram = promauto.NewHistogramVec(
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
		[]string{"experiment", "datatype", "year"},
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
	//           "ndt", "ndt5", "2011").Observe(bytes)
	BytesPerDateHistogram = promauto.NewHistogramVec(
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
		[]string{"experiment", "datatype", "year"},
	)

	// QueryCostHistogram tracks the costs of dedup and other queries.
	QueryCostHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "query_cost_seconds",
			Help: "bigquery query cost in slot seconds",
			Buckets: []float64{
				1.0, 2.15, 4.64, 10, 21.5, 46.4,
				100, 215, 464, 1000, 2150, 4640,
				10000, 21500, 46400, 100000, 215000, 464000, // ~120 hours
				1000000, 2150000, 4640000, 10000000, 21500000, 46400000, // ~12K hours
				// Some queries may run 100s of slot hours.  10K hours is likely
				// excessive, but it would be annoying if the top end were missed.
			},
		},
		// Worker type, e.g. ndt, sidestream, ptr, etc.
		[]string{"datatype", "query"},
	)
)
