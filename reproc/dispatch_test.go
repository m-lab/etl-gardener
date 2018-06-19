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

//===================================================
type testSaver struct {
	tasks  map[string][]state.Task
	delete map[string]struct{}
}

func (s *testSaver) SaveTask(t state.Task) error {
	//log.Println(t)
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(t state.Task) error {
	//log.Println("Delete:", t)
	s.delete[t.Name] = struct{}{}
	return nil
}

func assertSaver() { func(ex state.Saver) {}(&testSaver{}) }

type Exec struct{}

func (ex *Exec) DoAction(t *state.Task, terminate <-chan struct{}) {
	log.Println("Do", t)
	time.Sleep(time.Duration(1+rand.Intn(2)) * time.Millisecond)
}

func (ex *Exec) AdvanceState(t *state.Task) {
	switch t.State {
	case state.Invalid:
		t.State = state.Initializing
	case state.Initializing:
		t.State = state.Queuing
	case state.Queuing:
		t.State = state.Processing
	case state.Processing:
		t.Queue = "" // No longer need to keep the queue.
		t.State = state.Stabilizing
	case state.Stabilizing:
		t.State = state.Deduplicating
	case state.Deduplicating:
		t.State = state.Finishing
	case state.Finishing:
		t.State = state.Done
	case state.Done:
		// Generally shouldn't happen.
		// Do nothing
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
	th := reproc.NewTaskHandler(&exec, []string{})

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
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"})

	th.AddTask("a")

	go th.AddTask("b")
	go th.AddTask("c")

	time.Sleep(15 * time.Millisecond)
	th.Terminate()
	th.Wait()
}
