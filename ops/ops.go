// Package ops provides code that observes the tracker state, and takes appropriate actions.
package ops

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/m-lab/go/logx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/tracker"
)

/*
This package defines Action and Monitor.  Actions are applied to jobs in matching States.
The jJb is "locked" to prevent competing actions, a ConditionFunc is executed to determine
if the Job satisfies the preconditions, and then the OpFunc is applied, which should
ultimately update the Job's State.

A Monitor is initialized by adding Actions for each State that should be handled.  The client
code should then invoke "go Watch(...)" to start watching the tracker for eligible Jobs.

*/

var debug = logx.Debug

// A ConditionFunc checks whether a Job meets some condition.
// These functions may take a long time to complete, but should NOT use a lot of resources.
type ConditionFunc = func(ctx context.Context, job tracker.Job) bool

// An OpFunc performs an operation on a job, and updates its state.
// These functions may take a long time to complete, and may be resource intensive.
type OpFunc = func(ctx context.Context, tr *tracker.Tracker, job tracker.Job, status tracker.Status)

// An Action describes an operation to be applied to jobs that meet the required condition.
type Action struct {
	state      tracker.State // State that action applies to.
	condition  ConditionFunc // Condition that must be satisfied before applying action.
	op         OpFunc        // Action to apply.
	annotation string        // Annotation to be used for UpdateDetail while applying Op
}

// Monitor "owns" all jobs in the states that have actions.
type Monitor struct {
	bqconfig cloud.BQConfig
	actions  map[tracker.State]Action

	lock       sync.Mutex
	activeJobs map[tracker.Job]struct{} // The jobs currently being acted on.
}

func (m *Monitor) lockJob(j tracker.Job) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.activeJobs[j]; ok {
		return false
	}
	//debug.Println("Locking", j)
	m.activeJobs[j] = struct{}{}
	return true
}

func (m *Monitor) unlocker(j tracker.Job) func() {
	return func() {
		//debug.Println("unlocking", j)
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m.activeJobs, j)
	}
}

// AddAction adds a specific action to the Monitor.
func (m *Monitor) AddAction(state tracker.State, cond ConditionFunc, op OpFunc, annotation string) {
	m.actions[state] = Action{state, cond, op, annotation}
}

// Watch polls the tracker, and takes appropriate actions.
func (m *Monitor) Watch(ctx context.Context, tk *tracker.Tracker, period time.Duration) {
	ticker := time.NewTicker(period)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			debug.Println("================")
			// These jobs may be deleted by other calls to GetAll, so tk.UpdateJob may fail.
			jobs := tk.GetAll()
			for j, s := range jobs {
				if a, ok := m.actions[s.State]; ok {
					// If job is not already active.
					if m.lockJob(j) {
						go func(j tracker.Job, s tracker.Status, a Action, unlocker func()) {
							defer unlocker()
							if a.condition(ctx, j) {
								// These jobs may be deleted by other calls to GetAll, so tk.UpdateJob may fail.
								// s.UpdateDetail = a.annotation
								// tk.UpdateJob(j, s)
								// The op should also update the job state, detail, and error.
								if a.op != nil {
									a.op(ctx, tk, j, s)
								}
							}
						}(j, s, a, m.unlocker(j))
					}
				}
			}
		}
	}
}

func trueCondition(ctx context.Context, j tracker.Job) bool {
	return true
}

func newStateFunc(state tracker.State) OpFunc {
	return func(ctx context.Context, tk *tracker.Tracker, j tracker.Job, s tracker.Status) {
		debug.Println(j, state)
		err := tk.SetStatus(j, state)
		if err != nil {
			log.Println(err)
		}
	}
}

// NewMonitor creates a Monitor with no Actions
func NewMonitor(config cloud.BQConfig) *Monitor {
	m := Monitor{bqconfig: config, actions: make(map[tracker.State]Action, 10), activeJobs: make(map[tracker.Job]struct{}, 10)}
	return &m
}
