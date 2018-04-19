package state_test

import (
	"errors"
	"log"
	"testing"

	"github.com/m-lab/etl-gardener/state"
)

type saver struct {
	tasks  map[string][]state.Task
	delete map[string]struct{}
}

func (s *saver) SaveTask(t state.Task) error {
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *saver) DeleteTask(t state.Task) error {
	log.Println(t.Name)
	s.delete[t.Name] = struct{}{}
	return nil
}

func assertSaver() { func(ex state.Saver) {}(&saver{}) }

func TestTaskBasics(t *testing.T) {
	task := state.Task{Name: "foobar"}
	saver := saver{make(map[string][]state.Task), make(map[string]struct{})}
	task.SetSaver(&saver)

	task.Update(state.Initializing)

	task.Queue = "queue"
	task.Update(state.Queuing)

	tasks, ok := saver.tasks["foobar"]
	if !ok {
		t.Fatal("Should have an entry for foobar")
	}
	if len(tasks) != 2 {
		t.Fatal("Something very wrong")
	}
	if tasks[1].State != state.Queuing {
		t.Error("Should be queuing", tasks[1])
	}

	task.SetError(errors.New("test error"), "test")
	tasks, ok = saver.tasks["foobar"]
	if !ok {
		t.Fatal("Should have an entry for foobar")
	}
	if len(tasks) != 3 {
		t.Fatal("Something very wrong")
	}
	if tasks[2].State != state.Queuing {
		t.Error("Should be queuing", tasks[2])
	}
	if tasks[2].Err.Error() != "test error" {
		t.Error("Should have error", tasks[2])
	}
	if tasks[2].ErrInfo != "test" {
		t.Error("Should have error", tasks[2])
	}

	task.Delete()
	_, ok = saver.delete["foobar"]
	if !ok {
		t.Fatal("Should have called delete")
	}

}
