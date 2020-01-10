// Package ops provides code that observes the tracker state, and takes appropriate actions.
// It basically implements a simple state machine.

// The package assumes that there is only one Action associated with a State, and
// that States with Actions are never operated on independently by some other agent,
// such as the Parser.  Should this cease to be true, then the claim mechanism
// should be moved into the tracker.
package ops

import (
	"context"
	"sync"
	"time"

	"github.com/m-lab/go/logx"

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
*/

var debug = logx.Debug

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

// Monitor "owns" all jobs in the states that have actions.
type Monitor struct {
	bqconfig cloud.BQConfig           // static after creation
	actions  map[tracker.State]Action // static after creation

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
				a.action(ctx, m.tk, j, s)
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
			return

		case <-ticker.C:
			debug.Println("===== Monitor Loop Starting =====")
			// These jobs may be deleted by other calls to GetAll, so tk.UpdateJob may fail.
			jobs := m.tk.GetAll()
			// Iterate over the job/status map...
			for j, s := range jobs {
				// If job is in a state that has an associated action...
				if a, ok := m.actions[s.State]; ok {
					m.tryApplyAction(ctx, a, j, s)
				}
			}
		}
	}
}

// NewMonitor creates a Monitor with no Actions
func NewMonitor(config cloud.BQConfig, tk *tracker.Tracker) *Monitor {
	m := Monitor{bqconfig: config, actions: make(map[tracker.State]Action),
		tk: tk, jobClaims: make(map[tracker.Job]struct{})}
	return &m
}
