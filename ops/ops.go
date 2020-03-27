// Package ops provides code that observes the tracker state, and takes appropriate actions.
// It basically implements a simple state machine.
package ops

import (
	"context"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/dataset"
	"github.com/m-lab/go/logx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/tracker"
)

/*
This package defines Action and Monitor.  Actions are applied to jobs in matching States.
The Job is "locked" to prevent competing actions, a ConditionFunc is executed to determine
if the Job satisfies the preconditions, and then the OpFunc is applied, which should
ultimately update the Job's State.

A Monitor is initialized by adding Actions for each State that should be handled.  The client
code should then invoke "go Watch(...)" to start watching the tracker for eligible Jobs.

The package assumes that there is only one Action associated with a State, and
that States with Actions are never operated on independently by some other agent,
such as the Parser.  Should this cease to be true, then the claim mechanism
should be moved into the tracker.

Note that when Gardener restarts, it fetches the current state from datastore, and
creates the monitor.  In the new monitor, none of the tracker items will have leases,
so the Monitor will automatically restart the appropriate Actions.

TODO - Actions must all be recoverable - that is, if Gardener is terminated during
an Action, it should recover when Gardener restarts the action on startup.
*/

var debug = logx.Debug

var actionDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "gardener_action_duration",
		Help: "Histogram of duration of actions",
		Buckets: []float64{
			0.1, 0.14, 0.20, 0.27, 0.37, 0.52, 0.72,
			1, 1.4, 2.0, 2.7, 3.7, 5.2, 7.2,
			10, 14, 20, 27, 37, 52, 72,
			100, 140, 200, 270, 370, 520, 720,
			1000, 1400, 2000, 2700, 3700, 5200, 7200,
			10000, 14000, 20000, 27000, 37000, 52000, 72000,
		},
	},
	[]string{"action"},
)

// A ConditionFunc checks whether a Job meets some condition.
// These functions may take a long time to complete, but should NOT use a lot of resources.
type ConditionFunc = func(ctx context.Context, job tracker.Job) bool

// An ActionFunc performs an operation on a job, and updates its state.
// These functions may take a long time to complete, and may be resource intensive.
type ActionFunc = func(ctx context.Context, tr *tracker.Tracker, job tracker.Job, status tracker.Status)

// An Action describes an operation to be applied to jobs that meet the required condition.
type Action struct {
	state tracker.State // State that action applies to.

	// condition or action may be nil if not required
	// condition that must be satisfied before applying action.
	condition ConditionFunc
	// action performs an action on the job, and
	// updates the state when complete.  It may place the job into an error state.
	action ActionFunc

	annotation string // Annotation to be used for UpdateDetail while applying Op
}

// Name returns the name of the state that the action applies to.
func (a Action) Name() string {
	return string(a.state)
}

// Monitor "owns" all jobs in the states that have actions.
type Monitor struct {
	// TODO add bqClient, to allow fakes for testing.
	bqconfig cloud.BQConfig  // static after creation
	batchDS  dataset.Dataset // static after creation
	finalDS  dataset.Dataset // static after creation

	// TODO allow different action map for each datatype?
	actions map[tracker.State]Action // static after creation

	tk *tracker.Tracker

	lock      sync.Mutex               // protects jobClaims
	jobClaims map[tracker.Job]struct{} // Claimed jobs currently being acted on.
}

// releaser creates a function that releases the claim on a job.
func (m *Monitor) releaser(j tracker.Job) func() {
	return func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.jobClaims, j)
	}
}

// Returns releaser if successful, nil otherwise.
func (m *Monitor) tryClaimJob(j tracker.Job) func() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.jobClaims[j]; ok {
		return nil
	}
	m.jobClaims[j] = struct{}{}
	return m.releaser(j)
}

// AddAction adds a specific action to the Monitor.
func (m *Monitor) AddAction(state tracker.State, cond ConditionFunc, op ActionFunc, annotation string) {
	m.actions[state] = Action{state, cond, op, annotation}
}

// applyAction tries to claim a job and apply an action.  Returns false if the job is already claimed.
func (m *Monitor) tryApplyAction(ctx context.Context, a Action, j tracker.Job, s tracker.Status) bool {
	// If job is not already claimed.
	releaser := m.tryClaimJob(j)
	if releaser == nil {
		return false
	}
	go func(j tracker.Job, s tracker.Status, a Action, releaser func()) {
		defer releaser()
		if a.condition == nil || a.condition(ctx, j) {
			// These jobs may be deleted by other calls to GetAll, so tk.UpdateJob may fail.
			// s.UpdateDetail = a.annotation
			// tk.UpdateJob(j, s)
			// The op should also update the job state, detail, and error.
			if a.action != nil {
				start := time.Now()
				a.action(ctx, m.tk, j, s)
				actionDuration.WithLabelValues(a.Name()).Observe(time.Since(start).Seconds())
			}
		}
	}(j, s, a, releaser)
	return true
}

// Watch polls the tracker, and takes appropriate actions.
func (m *Monitor) Watch(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Monitor.Watch terminating")
			return

		case <-ticker.C:
			debug.Println("===== Monitor Loop Starting =====")
			// These jobs may be deleted by other calls to GetAll, so tk.UpdateJob may fail.
			jobs, _, _ := m.tk.GetState()
			// Iterate over the job/status map...
			for j, s := range jobs {
				// If job is in a state that has an associated action...
				if a, ok := m.actions[s.LastState().State]; ok {
					m.tryApplyAction(ctx, a, j, s)
				}
			}
		}
	}
}

// NewMonitor creates a Monitor with no Actions
func NewMonitor(clientCtx context.Context, config cloud.BQConfig, tk *tracker.Tracker) (*Monitor, error) {
	// Code snippet adapted from dataset.NewDataset
	c, err := bigquery.NewClient(clientCtx, config.BQProject, config.Options...)
	if err != nil {
		return nil, err
	}
	bqClient := bqiface.AdaptClient(c)
	batchDS := dataset.Dataset{Dataset: bqClient.Dataset(config.BQBatchDataset), BqClient: bqClient}
	finalDS := dataset.Dataset{Dataset: bqClient.Dataset(config.BQFinalDataset), BqClient: bqClient}

	m := Monitor{bqconfig: config, batchDS: batchDS, finalDS: finalDS, actions: make(map[tracker.State]Action),
		tk: tk, jobClaims: make(map[tracker.Job]struct{})}
	return &m, nil
}
