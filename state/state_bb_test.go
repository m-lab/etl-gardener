package state_test

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type S struct{}

func (s *S) SaveTask(t state.Task) error   { return nil }
func (s *S) DeleteTask(t state.Task) error { return nil }

func (s *S) SaveSystem(ss *state.SystemState) error { return nil }

func assertSaver() { func(ex state.Saver) {}(&S{}) }

type Exec struct{}

func (ex *Exec) DoAction(t *state.Task, terminate <-chan struct{}) {
	log.Printf("%+v\n", *t)
	time.Sleep(time.Duration(1+rand.Intn(2)) * time.Millisecond)
}

func (ex *Exec) AdvanceState(t *state.Task) {
	switch t.State {
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

type L struct{}

func (ldr *L) Fetch(taskName string) state.Task {
	return state.Task{Name: taskName, Queue: "Q-" + taskName}
}

func (ldr *L) LoadFromPS(ss *state.SystemState) {
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

func TestBasic(t *testing.T) {
}
