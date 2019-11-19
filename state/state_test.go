package state_test

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"

	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/go/dataset"
)

type Task = state.Task

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type testSaver struct {
	lock   sync.Mutex
	tasks  map[string][]Task
	delete map[string]struct{}
}

func (s *testSaver) SaveTask(ctx context.Context, t Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(ctx context.Context, t Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.delete[t.Name] = struct{}{}
	return nil
}

func (s *testSaver) GetTasks() map[string][]Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	m := make(map[string][]Task, len(s.tasks))
	for k, v := range s.tasks {
		m[k] = v
	}
	return m
}

func (s *testSaver) GetDeletes(t Task) map[string]struct{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.delete
}

func assertSaver() { func(ex state.PersistentStore) {}(&testSaver{}) }

func TestNewPlatformPrefix(t *testing.T) {
	task := Task{Name: "gs://pusher-mlab-sandbox/ndt/tcpinfo/2019/04/01/", State: state.Initializing}
	saver := testSaver{tasks: make(map[string][]Task), delete: make(map[string]struct{})}
	task.SetSaver(&saver)

	pp, err := task.ParsePrefix()
	if err != nil {
		t.Fatal(err)
	}

	if pp.DataType != "tcpinfo" {
		t.Error(pp)
	}

}

func TestLegacyPrefix(t *testing.T) {
	task := Task{Name: "gs://archive-mlab-sandbox/ndt/2019/04/01/", State: state.Initializing}
	saver := testSaver{tasks: make(map[string][]Task), delete: make(map[string]struct{})}
	task.SetSaver(&saver)

	pp, err := task.ParsePrefix()
	if err != nil {
		t.Fatal(err)
	}

	if pp.DataType != "ndt" {
		t.Error(pp)
	}

}

func TestTaskBasics(t *testing.T) {
	ctx := context.Background()
	task := Task{Name: "foobar", State: state.Initializing}
	saver := testSaver{tasks: make(map[string][]Task), delete: make(map[string]struct{})}
	task.SetSaver(&saver)

	task.Update(ctx, state.Initializing)

	task.Queue = "queue"
	task.Update(ctx, state.Queuing)

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

	task.SetError(ctx, errors.New("test error"), "test")
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

	task.Delete(ctx)
	_, ok = saver.delete["foobar"]
	if !ok {
		t.Fatal("Should have called delete")
	}

	if err := task.Save(ctx); err != nil {
		t.Fatal("Should not have an error here", err)
	}

	task.SetSaver(nil)
	if err := task.Save(ctx); err != state.ErrNoSaver {
		t.Fatal("Should have gotten ErrNoSaver but instead got", err)
	}
	if err := task.Delete(ctx); err != state.ErrNoSaver {
		t.Fatal("Should have gotten ErrNoSaver but instead got", err)
	}
	if err := task.Update(ctx, state.Queuing); err != state.ErrNoSaver {
		t.Fatal("Should have gotten ErrNoSaver but instead got", err)
	}
	if err := task.SetError(ctx, nil, ""); err != state.ErrNoSaver {
		t.Fatal("Should have gotten ErrNoSaver but instead got", err)
	}
	if len(task.String()) < 1 {
		t.Fatal("The string should exist.")
	}
}

func TestSourceAndDest(t *testing.T) {
	if _, err := state.NewTask("exp", "gs://task1", "Q1", nil); err == nil {
		t.Fatal("Should have had an error here")
	}
	if _, err := state.NewTask("exp", "gs://foo/ndt/2000/ab/01/task1", "Q1", nil); err == nil {
		t.Fatal("Should have had an error here")
	}
	if _, err := state.NewTask("exp", "gs://foo/ndt/2000/13/01/task1", "Q1", nil); err == nil {
		t.Fatal("Should have had an error here")
	}

	ctx := context.Background()
	task, err := state.NewTask("exp", "gs://foo/ndt/2000/01/01/task1", "Q1", nil)
	if err != nil {
		t.Fatal(err)
	}

	dsExt, err := dataset.NewDataset(ctx, "mlab-testing", "dataset")
	if err != nil {
		t.Fatal(err)
	}

	src, dest, err := task.SourceAndDest(&dsExt)
	if err != nil {
		t.Fatal(err)
	}
	// Source should be a templated table, ending in _date.
	if src.FullyQualifiedName() != "mlab-testing:dataset.ndt_20000101" {
		t.Error(src.FullyQualifiedName())
	}
	// Source should be a partition, ending in $date.
	if dest.FullyQualifiedName() != "mlab-testing:dataset.ndt$20000101" {
		t.Error(dest.FullyQualifiedName())
	}

	// Exercise error case, which should be impossible if the task was created with NewTask.
	task = &Task{Name: "gs://foo/ndt/2000/ab/01/task1"}
	_, _, err = task.SourceAndDest(&dsExt)
	if err == nil {
		t.Fatal("Should have had an error")
	}
}

func TestGetExperiment(t *testing.T) {
	expt, err := state.GetExperiment("gs://archive-mlab-oti/ndt/2017/06/01/")
	if expt != "ndt" || err != nil {
		t.Error(err)
	}
}
