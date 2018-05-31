package rex_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/rex"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

//===================================================
type SS struct{} // SystemSaver

func (s *SS) SaveSystem(ss *rex.ReprocState) error {
	log.Println("Save:", ss)
	return nil
}

func assertSysSaver() { func(ex rex.SysSaver) {}(&SS{}) }

type S struct{}

func (s *S) SaveTask(t state.Task) error {
	log.Println(t)
	return nil
}

func (s *S) DeleteTask(t state.Task) error {
	log.Println("Delete:", t)
	return nil
}

func assertSaver2() { func(ex state.Saver) {}(&S{}) }

type Exec struct{}

func (ex *Exec) DoAction(t *state.Task, terminate <-chan struct{}) {
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
	}
}

func AssertExecutor() { func(ex state.Executor) {}(&Exec{}) }

type L struct{}

func (ldr *L) Fetch(taskName string) state.Task {
	return state.Task{Name: taskName, Queue: "Q-" + taskName}
}

func (ldr *L) LoadFromPS(ss *rex.ReprocState) {
	log.Println("Load")
	var err error
	ss.NextDate, err = time.Parse("20060102", "20170601")

	if err != nil {
		log.Fatal(err)
	}

	for name := range ss.AllTasks {
		log.Println("Starting", name)
		// Create new task
		t := ldr.Fetch(name)
		ss.Add(t)
	}
}

func AssertLoader() { func(ex rex.Loader) {}(&L{}) }

func TestBasic(t *testing.T) {
	ss := rex.LoadAndInitReprocState(&L{}, &Exec{}, &S{}, &SS{})
	if len(ss.AllTasks) != 0 {
		t.Fatal("error", ss.AllTasks)
	}
	go ss.DoDispatchLoop("Fake", []string{"ndt"}, time.Now().Add(-10*24*time.Hour))

	rex.GetQueueChan(ss) <- "Q1"
	rex.GetQueueChan(ss) <- "Q2"
	time.Sleep(20 * time.Millisecond)
	ss.Terminate()
	ss.WaitForTerminate()
}
