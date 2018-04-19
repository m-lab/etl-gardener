package dispatch_test

import (
	"log"
	"testing"
	"time"

	"google.golang.org/api/option"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/dispatch"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type S struct {
	tasks map[string][]state.Task
}

func (s *S) SaveTask(t state.Task) error {
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *S) DeleteTask(t state.Task) error { return nil }

func assertSaver() { func(ex state.Saver) {}(&S{}) }

func TestDispatcherLifeCycle(t *testing.T) {
	// Use a fake client so we intercept all the http ops.
	client, counter := tq.DryRunQueuerClient()

	saver := S{tasks: make(map[string][]state.Task)}

	// With time.Now(), this shouldn't send any requests.
	// Inject fake client for bucket ops, so it doesn't hit the backends at all.
	// Construction will trigger 4 HTTP calls, one to check that each queue is empty.
	d, err := dispatch.NewDispatcher(client, "project", "queue-base-", 4, time.Now(), &saver, option.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}

	// Add will trigger 1 HTTP call to post the task.
	d.Add("gs://foobar/ndt/2001/01/01/")

	// This waits for all work to complete, then closes all channels.
	// We won't terminate until all handlers have finished, and one of them
	// should trigger 1 HTTP call as part of next isEmpty check.
	d.Terminate()

	// Count should be 6 (HTTP gets)
	if counter.Count() != 6 {
		t.Errorf("Count was %d instead of 6", counter.Count())
	}

	err = d.Add("gs://foobar/ndt/2001/01/02/")
	if err != dispatch.ErrTerminating {
		t.Error("Should get", dispatch.ErrTerminating)
	}

	taskStates, ok := saver.tasks["gs://foobar/ndt/2001/01/01/"]
	if !ok {
		t.Fatal("Task not saved")
	}
	// For now, state should have progressed to Stabilizing.
	if len(taskStates) != 4 {
		t.Fatal("Wrong number of states:", len(taskStates))
	}

	if taskStates[1].State != state.Queuing {
		t.Errorf("Wrong state %+v\n", taskStates[1])
	}

	if taskStates[2].State != state.Processing {
		t.Errorf("Wrong state %+v\n", taskStates[2])
	}

	if taskStates[3].State != state.Stabilizing {
		t.Errorf("Wrong state %+v\n", taskStates[3])
	}

}
