package state_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

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

func CleanupDatastore(ds *state.DatastoreSaver) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	q := datastore.NewQuery("").Namespace(ds.Namespace)
	keys, err := ds.Client.GetAll(ctx, q.KeysOnly(), nil)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err = ds.Client.DeleteMulti(ctx, keys)
	if err != nil {
		return err
	}
	return ctx.Err()
}

func TestStatus(t *testing.T) {
	saver, err := state.NewDatastoreSaver("mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	task := state.Task{Name: "task1", Queue: "Q1", State: state.Initializing}
	task.SetSaver(saver)
	log.Println("saving")
	err = task.Save()
	if err != nil {
		t.Fatal(err)
	}
	task.Name = "task2"
	task.Queue = "Q2"
	err = task.Update(state.Queuing)
	if err != nil {
		t.Fatal(err)
	}

	// Real datastore takes about 100 msec or more before consistency.
	// In travis, we use the emulator, which should provide consistency
	// much more quickly.  So we use a modest number here that usually
	// is sufficient for running on workstation.
	time.Sleep(200 * time.Millisecond)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tasks, err := saver.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	if len(tasks) != 2 {
		t.Error("Should be 2 tasks", len(tasks))
		for _, t := range tasks {
			log.Println(t)
		}
	}
	err = CleanupDatastore(saver)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteStatus(t *testing.T) {
	saver, err := state.NewDatastoreSaver("mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	task := state.Task{Name: "task1", Queue: "Q1", State: state.Initializing}
	task.SetSaver(saver)
	task.Save()
	task.Name = "task2"
	task.Queue = "Q2"
	task.Update(state.Queuing)
	time.Sleep(200 * time.Millisecond)

	bb := make([]byte, 0, 500)
	buf := bytes.NewBuffer(bb)

	err = state.WriteHTMLStatusTo(buf, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "task1") {
		t.Error("Missing task1")
	}
	if !strings.Contains(buf.String(), "task2") {
		t.Error("Missing task2")
	}

	err = CleanupDatastore(saver)
	if err != nil {
		t.Fatal(err)
	}
}
