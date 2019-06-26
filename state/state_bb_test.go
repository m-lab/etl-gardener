// +build integration

package state_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/state"
)

func waitForNTasks(t *testing.T, saver *state.DatastoreSaver, expectedTaskCount int, expt string) []state.Task {
	var tasks []state.Task
	var err error
	for i := 0; i < 50; i++ {
		// Real datastore takes about 100 msec or more before consistency.
		// In travis, we use the emulator, which should provide consistency
		// much more quickly.  We use a modest number here that usually
		// is sufficient for running on workstation, and rarely fail with emulator.
		// Then we retry up to 50 times, before actually failing.
		time.Sleep(100 * time.Millisecond)
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		tasks, err = saver.FetchAllTasks(ctx, expt)
		if err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		if len(tasks) >= expectedTaskCount {
			break
		}
	}
	return tasks
}

func TestTaskState(t *testing.T) {
	ctx := context.Background()
	saver, err := state.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		log.Println(saver)
		t.Fatal(err)
	}
	task, err := state.NewTask("exp", "gs://foo/bar/2000/01/01/task1", "Q1", saver)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("saving")
	err = task.Save(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task.Name = "gs://foo/bar/2000/01/01/task2"
	task.Queue = "Q2"
	err = task.Update(ctx, state.Queuing)
	if err != nil {
		t.Fatal(err)
	}

	ExpectedTasks := 2
	tasks := waitForNTasks(t, saver, ExpectedTasks, "exp")
	if len(tasks) != ExpectedTasks {
		t.Errorf("Saw %d tasks instead of %d (see notes on consistency)", len(tasks), ExpectedTasks)
		for _, t := range tasks {
			log.Println(t)
		}
	}

	// Check the tasks individually:
	for _, oneTask := range tasks {
		task, err := saver.FetchTask(ctx, "exp", oneTask.Name)
		if err != nil {
			t.Error(err)
		} else {
			if task.State != oneTask.State {
				t.Error("State doesn't match", task, oneTask)
			}
			// task doesn't have a saver, so this should return an empty task.
			t2, err := task.GetTaskStatus(ctx)
			if err != nil {
				t.Error("Shouldn't return an error", err)
			}
			if t2.State != state.Invalid {
				t.Error("Unexpected state in ", t2)
			}
			task.SetSaver(saver)
			t2, err = task.GetTaskStatus(ctx)
			if err != nil {
				t.Error("Shouldn't return an error", err)
			}
			if t2.State != task.State {
				t.Error("Expected", task, "but got", t2)
			}
		}
	}

	for i := range tasks {
		err := saver.DeleteTask(ctx, tasks[i])
		if err != nil {
			t.Error(err)
		}
	}
}

func TestFetchTask(t *testing.T) {
	ctx := context.Background()
	saver, err := state.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		log.Println(saver)
		t.Fatal(err)
	}

	_, err = saver.FetchTask(ctx, "exp", "no-such-entity")
	if err != datastore.ErrNoSuchEntity {
		t.Error("Expected ErrNoSuchEntity", err)
	}

	noSuchEntity, err := state.NewTask("exp,", "gs://foo/bar/0001/01/01/no-such-entity", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// task doesn't have a saver - should return an empty task.
	_, err = noSuchEntity.GetTaskStatus(ctx)
	if err != nil {
		t.Error("Shouldn't return an error", err)
	}

	// with a saver - still should return an empty task without error.
	noSuchEntity.SetSaver(saver)
	_, err = noSuchEntity.GetTaskStatus(ctx)
	if err != nil {
		t.Error("Shouldn't return an error", err)
	}

	// Now save a real test.
	task, err := state.NewTask("exp", "gs://foo/bar/2000/01/01/task1", "Q1", saver)
	if err != nil {
		t.Fatal(err)
	}

	log.Println("saving")
	err = task.Save(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = saver.FetchTask(ctx, task.Experiment, task.Name)
	if err != nil {
		t.Error(err)
	}

	saved, err := task.GetTaskStatus(ctx)
	if err != nil {
		t.Error("Shouldn't return an error", err)
	}
	if saved.Name != task.Name {
		t.Error("Unexpected state", saved)
	}

	err = saver.DeleteTask(ctx, *task)
	if err != nil {
		t.Error(err)
	}
}

func TestWriteStatus(t *testing.T) {
	ctx := context.Background()
	saver, err := state.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	task, err := state.NewTask("bar", "gs://foo/bar/2000/01/01/task1", "Q1", saver)
	if err != nil {
		t.Fatal(err)
	}
	task.Save(ctx)
	t1 := task
	task.Name = "task2"
	task.Experiment = "bar"
	task.Queue = "Q2"
	task.Update(ctx, state.Queuing)
	t2 := task

	ExpectedTasks := 2
	waitForNTasks(t, saver, ExpectedTasks, "bar")

	bb := make([]byte, 0, 500)
	buf := bytes.NewBuffer(bb)

	err = state.WriteHTMLStatusTo(ctx, buf, "mlab-testing", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "task1") {
		t.Error("Missing task1")
	}
	if !strings.Contains(buf.String(), "task2") {
		t.Error("Missing task2")
	}

	err = t1.Delete(ctx)
	if err != nil {
		t.Error(err)
	}
	err = t2.Delete(ctx)
	if err != nil {
		t.Error(err)
	}
}
