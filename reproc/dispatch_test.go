package reproc_test

import (
	"context"
	"log"
	"math/rand"
	"sync"
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
	lock   sync.Mutex
}

func NewTestSaver() *testSaver {
	return &testSaver{make(map[string][]state.Task, 20), make(map[string]struct{}, 20), sync.Mutex{}}
}

func (s *testSaver) SaveTask(t state.Task) error {
	//log.Println(t)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(t state.Task) error {
	//log.Println("Delete:", t)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.delete[t.Name] = struct{}{}
	return nil
}

func assertSaver() { func(ex state.Saver) {}(&testSaver{}) }

type Exec struct{}

func (ex *Exec) Next(ctx context.Context, t *state.Task, terminate <-chan struct{}) error {
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
	return nil
}

func AssertExecutor() { func(ex state.Executor) {}(&Exec{}) }

// This test exercises the task management, including invoking t.Process().
//  It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestBasic(t *testing.T) {
	ctx := context.Background()
	// Start tracker with no queues.
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{}, saver)

	// This will block because there are no queues.
	go th.AddTask(ctx, "foobar")
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
	ctx := context.Background()
	// Start tracker with one queue.
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"}, saver)

	th.AddTask(ctx, "gs://fake/ndt/2017/09/22/")

	go th.AddTask(ctx, "gs://fake/ndt/2017/09/24/")
	go th.AddTask(ctx, "gs://fake/ndt/2017/09/26/")

	time.Sleep(15 * time.Millisecond)
	th.Terminate()
	th.Wait()
}

func TestRestart(t *testing.T) {
	ctx := context.Background()
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{"queue-1", "queue-2"}, saver)

	taskName := "gs://foobar/exp/2001/02/03/"
	t1, err := state.NewTask(taskName, "queue-1", nil)
	t1.State = state.Processing
	if err != nil {
		t.Fatal(err)
	}
	tasks := []state.Task{*t1}
	th.RestartTasks(ctx, tasks)

	time.Sleep(5 * time.Second)
	log.Println(saver.tasks[taskName])
}
