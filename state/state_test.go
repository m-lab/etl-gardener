package state_test

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type testSaver struct {
	tasks  map[string][]state.Task
	delete map[string]struct{}
}

func (s *testSaver) SaveTask(t state.Task) error {
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(t state.Task) error {
	s.delete[t.Name] = struct{}{}
	return nil
}

func assertSaver() { func(ex state.Saver) {}(&testSaver{}) }

func TestTaskBasics(t *testing.T) {
	task := state.Task{Name: "foobar", State: state.Initializing}
	saver := testSaver{make(map[string][]state.Task), make(map[string]struct{})}
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

func CleanupDatastore(ds *state.DatastoreSaver) {
	ctx := context.Background()
	q := datastore.NewQuery("").Namespace(ds.Namespace)
	keys, err := ds.Client.GetAll(ctx, q.KeysOnly(), nil)
	if err != nil {
		log.Fatal("Failed cleanup", err)
	}
	err = ds.Client.DeleteMulti(ctx, keys)
	if err != nil {
		log.Fatal("Failed cleanup", err)
	}
}

func TestStatus(t *testing.T) {
	os.Setenv("PROJECT", "xyz")
	saver, err := state.NewDatastoreSaver()
	if err != nil {
		t.Fatal(err)
	}
	task := state.Task{Name: "task1", Queue: "Q1", State: state.Initializing}
	task.SetSaver(saver)
	task.Save()
	task.Name = "task2"
	task.Queue = "Q2"
	task.Update(state.Queuing)

	tasks, err := saver.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Error("Should be 2 tasks", len(tasks))
		for _, t := range tasks {
			log.Println(t)
		}
	}
	CleanupDatastore(saver)
}
