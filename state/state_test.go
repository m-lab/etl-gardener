package state_test

import (
	"errors"
	"log"
	"sync"
	"testing"

	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/go/bqext"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type testSaver struct {
	lock   sync.Mutex
	tasks  map[string][]state.Task
	delete map[string]struct{}
}

func (s *testSaver) SaveTask(t state.Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(t state.Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.delete[t.Name] = struct{}{}
	return nil
}

func (s *testSaver) GetTasks(t state.Task) map[string][]state.Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.tasks
}

func (s *testSaver) GetDeletes(t state.Task) map[string]struct{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.delete
}

func assertSaver() { func(ex state.Saver) {}(&testSaver{}) }

func TestTaskBasics(t *testing.T) {
	task := state.Task{Name: "foobar", State: state.Initializing}
	saver := testSaver{tasks: make(map[string][]state.Task), delete: make(map[string]struct{})}
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
	if tasks[2].ErrMsg != "test error" {
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

func TestSourceAndDest(t *testing.T) {
	task, err := state.NewTask("gs://foo/foobar/2000/01/01/task1", "Q1", nil)
	if err != nil {
		t.Fatal(err)
	}

	dsExt, err := bqext.NewDataset("mlab-testing", "dataset")
	if err != nil {
		t.Fatal(err)
	}

	src, dest, err := task.SourceAndDest(&dsExt)
	if err != nil {
		t.Fatal(err)
	}
	// Source should be a templated table, ending in _date.
	if src.FullyQualifiedName() != "mlab-testing:dataset.foobar_20000101" {
		t.Error(src.FullyQualifiedName())
	}
	// Source should be a partition, ending in $date.
	if dest.FullyQualifiedName() != "mlab-testing:dataset.foobar$20000101" {
		t.Error(dest.FullyQualifiedName())
	}
}
