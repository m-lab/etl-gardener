package rex_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud/tq"
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
	//log.Println("Save:", ss)
	return nil
}

func assertSysSaver() { func(ex rex.SysSaver) {}(&SS{}) }

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
	saver := testSaver{make(map[string][]state.Task), make(map[string]struct{})}
	ss := rex.LoadAndInitReprocState(&L{}, &Exec{}, &saver, &SS{})
	if len(ss.AllTasks) != 0 {
		t.Fatal("error", ss.AllTasks)
	}
	go ss.DoDispatchLoop("Fake", []string{"ndt"}, time.Now().Add(-10*24*time.Hour))

	rex.GetQueueChan(ss) <- "Q1"
	//rex.GetQueueChan(ss) <- "Q2"
	time.Sleep(20 * time.Millisecond)
	ss.Terminate()
	ss.WaitForTerminate()

	for i := range saver.tasks {
		log.Println(i, len(saver.tasks[i]), saver.tasks[i][len(saver.tasks[i])-1])
	}
	log.Printf("%v\n", saver.delete)
	log.Println(len(saver.tasks), len(saver.delete))
}

func TestActual(t *testing.T) {
	saver := testSaver{make(map[string][]state.Task), make(map[string]struct{})}
	client, counter := tq.DryRunQueuerClient()
	ss := rex.LoadAndInitReprocState(&L{}, &rex.ReprocessingExecutor{Client: client}, &saver, &SS{})
	if len(ss.AllTasks) != 0 {
		t.Fatal("error", ss.AllTasks)
	}
	go ss.DoDispatchLoop("Fake", []string{"ndt"}, time.Now().Add(-10*24*time.Hour))

	rex.GetQueueChan(ss) <- "Q1"
	//rex.GetQueueChan(ss) <- "Q2"
	time.Sleep(20 * time.Millisecond)
	ss.Terminate()
	ss.WaitForTerminate()

	for i := range saver.tasks {
		log.Println(i, len(saver.tasks[i]), saver.tasks[i][len(saver.tasks[i])-1])
	}
	log.Printf("%v\n", saver.delete)
	log.Println(len(saver.tasks), len(saver.delete))

	log.Println(counter)
}
