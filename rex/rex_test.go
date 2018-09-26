package rex_test

import (
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
	client, counter := cloud.DryRunClient()
	config := cloud.Config{Project: "mlab-testing", Client: client}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "bqproject", BQDataset: "dataset"}
	bucketOpts := []option.ClientOption{option.WithHTTPClient(client)}
	exec := rex.ReprocessingExecutor{BQConfig: bqConfig, BucketOpts: bucketOpts}
	saver := newTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"}, saver)

	th.AddTask("gs://foo/bar/2001/01/01/foobar-a.gz")

	go th.AddTask("gs://foo/bar/2001/01/01/foobar-b.gz")
	go th.AddTask("gs://foo/bar/2001/01/01/foobar-c.gz")

	for counter.Count() < 3 {
		time.Sleep(100 * time.Millisecond)
	}
	//	for len(saver.GetDeletes()) < 3 {
	//		time.Sleep(100 * time.Millisecond)
	//	}

	// th.Terminate()
	th.Wait()
	log.Println(counter.Count())
	log.Println("Deletes:", len(saver.GetDeletes()))
	log.Println("Tasks:", len(saver.GetTasks()))
}
