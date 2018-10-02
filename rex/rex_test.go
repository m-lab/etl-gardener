package rex_test

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"
	"github.com/m-lab/etl-gardener/state"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

type testSaver struct {
	lock   sync.Mutex
	tasks  map[string][]state.Task
	delete map[string]struct{}
}

func newTestSaver() *testSaver {
	return &testSaver{tasks: make(map[string][]state.Task), delete: make(map[string]struct{})}
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

func (s *testSaver) GetTasks() map[string][]state.Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.tasks
}

func (s *testSaver) GetDeletes() map[string]struct{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.delete
}

// This test exercises the task management, including invoking t.Process().
// It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
// TODO - use a fake bigtable, so that tasks can get beyond Stabilizing.
func TestWithTaskQueue(t *testing.T) {
	ctx := context.Background()
	client, counter := cloud.DryRunClient()
	config := cloud.Config{Project: "mlab-testing", Client: client}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "bqproject", BQBatchDataset: "dataset"}
	bucketOpts := []option.ClientOption{option.WithHTTPClient(client)}
	exec := rex.ReprocessingExecutor{BQConfig: bqConfig, BucketOpts: bucketOpts}
	saver := newTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"}, saver)

	th.AddTask(ctx, "gs://foo/bar/2001/01/01/")

	go th.AddTask(ctx, "gs://foo/bar/2001/01/02/")
	go th.AddTask(ctx, "gs://foo/bar/2001/01/03/")

	start := time.Now()
	for counter.Count() < 3 && time.Now().Before(start.Add(2*time.Second)) {
		time.Sleep(100 * time.Millisecond)
	}

	th.Wait()

	if counter.Count() != 3 {
		t.Error("Wrong number of client calls", counter.Count())
	}
	if len(saver.GetDeletes()) != 3 {
		t.Error("Wrong number of task deletes", len(saver.GetDeletes()))
	}

	tasks := saver.GetTasks()
	for _, task := range tasks {
		log.Println(task)
		if task[len(task)-1].State != state.Done {
			t.Error("Bad task state:", task)
		}
	}

}
