// +build integration

package state_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/state"
)

func waitForNTasks(t *testing.T, saver *state.DatastoreSaver, expectedTaskCount int, expt string) []state.Task {
	var tasks []state.Task
	var err error
	for i := 0; i < 10; i++ {
		// Real datastore takes about 100 msec or more before consistency.
		// In travis, we use the emulator, which should provide consistency
		// much more quickly.  We use a modest number here that usually
		// is sufficient for running on workstation, and rarely fail with emulator.
		// Then we retry up to 10 times, before actually failing.
		time.Sleep(200 * time.Millisecond)
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		tasks, err = saver.GetStatus(ctx, expt)
		if err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		if len(tasks) == expectedTaskCount {
			break
		}
	}
	return tasks
}

func TestStatus(t *testing.T) {
	ctx := context.Background()
	saver, err := state.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		log.Println(saver)
		t.Fatal(err)
	}
	task, err := state.NewTask("gs://foo/bar/2000/01/01/task1", "Q1", saver)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("saving")
	err = task.Save()
	if err != nil {
		t.Fatal(err)
	}
	task.Name = "gs://foo/bar/2000/01/01/task2"
	task.Queue = "Q2"
	err = task.Update(state.Queuing)
	if err != nil {
		t.Fatal(err)
	}

	ExpectedTasks := 2
	tasks := waitForNTasks(t, saver, ExpectedTasks, "bar")
	if len(tasks) != ExpectedTasks {
		t.Errorf("Saw %d tasks instead of %d (see notes on consistency)", len(tasks), ExpectedTasks)
		for _, t := range tasks {
			log.Println(t)
		}
	}

	for i := range tasks {
		err := saver.DeleteTask(tasks[i])
		if err != nil {
			t.Error(err)
		}
	}
}

func TestWriteStatus(t *testing.T) {
	ctx := context.Background()
	saver, err := state.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}
	task, err := state.NewTask("gs://foo/bar/2000/01/01/task1", "Q1", saver)
	if err != nil {
		t.Fatal(err)
	}
	task.Save()
	t1 := task
	task.Name = "task2"
	task.Experiment = "bar"
	task.Queue = "Q2"
	task.Update(state.Queuing)
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

	err = t1.Delete()
	if err != nil {
		t.Error(err)
	}
	err = t2.Delete()
	if err != nil {
		t.Error(err)
	}
}
