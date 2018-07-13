package reproc_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func assertTaskPipe(t state.Terminator) {
	func(t state.Terminator) {}(&reproc.Terminator{})
}

// This test exercises the termination sequencing.  It does not check
// any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestTerminator(t *testing.T) {
	trm := reproc.NewTerminator()
	notifier := trm.GetNotifyChannel()

	trm.Add(1)
	go func() {
		<-notifier
		trm.Done()
	}()
	trm.Add(1)
	go func() {
		<-notifier
		trm.Done()
	}()

	trm.Terminate()
	trm.Wait()
}

type Exec struct{}

func (ex *Exec) Next(t *state.Task, terminate <-chan struct{}) {
	log.Println("Do", t)
	time.Sleep(time.Duration(1+rand.Intn(2)) * time.Millisecond)

	switch t.State {
	case state.Invalid:
		t.Update(state.Initializing)
	case state.Initializing:
		t.Update(state.Queuing)
	case state.Queuing:
		t.Update(state.Processing)
	case state.Processing:
		t.Queue = "" // No longer need to keep the queue.
		t.Update(state.Stabilizing)
	case state.Stabilizing:
		t.Update(state.Deduplicating)
	case state.Deduplicating:
		t.Update(state.Finishing)
	case state.Finishing:
		t.Update(state.Done)
	case state.Done:
		// Generally shouldn't happen.
		// In prod, we would ignore this, but for test we log.Fatal to force
		// a test failure.
		log.Fatal("Should not call Next when state is Done")
	}
}

func AssertExecutor() { func(ex state.Executor) {}(&Exec{}) }

// This test exercises the task management, including invoking t.Process().
//  It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestBasic(t *testing.T) {
	// Start tracker with no queues.
	exec := Exec{}
	th := reproc.NewTaskHandler(&exec, []string{}, nil)

	// This will block because there are no queues.
	go th.AddTask("foobar")
	// Just so it is clear where the message comes from...
	time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)

	th.Terminate()
	th.Wait() // Race
}

// This test exercises the task management, including invoking t.Process().
// It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestWithTaskQueue(t *testing.T) {
	// Start tracker with one queue.
	exec := Exec{}
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"}, nil)
	th.AddTask("gs://fake/ndt/2017/09/22/")

	go th.AddTask("gs://fake/ndt/2017/09/24/")
	go th.AddTask("gs://fake/ndt/2017/09/26/")

	time.Sleep(15 * time.Millisecond)
	th.Terminate()
	th.Wait()
}
