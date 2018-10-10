package rex_test

import (
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"
	"github.com/m-lab/etl-gardener/state"
	"google.golang.org/api/iterator"
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

func (s *testSaver) SaveTask(ctx context.Context, t state.Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(ctx context.Context, t state.Task) error {
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

// NewReprocessingExecutor creates an exec with a fakeClient.
func NewReprocessingExecutor(ctx context.Context, config cloud.BQConfig, bucketOpts ...option.ClientOption) (*rex.ReprocessingExecutor, error) {
	return &rex.ReprocessingExecutor{BQConfig: config, StorageClient: fakeClient{}}, nil
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
	bqConfig := cloud.BQConfig{Config: config, BQProject: "bqproject", BQBatchDataset: "batch", BQFinalDataset: "final"}
	fc := fakeClient{objects: []*storage.ObjectAttrs{
		&storage.ObjectAttrs{Name: "obj1"},
		&storage.ObjectAttrs{Name: "obj2"},
	}}
	exec, err := rex.NewReprocessingExecutor(ctx, bqConfig)
	if err != nil {
		log.Fatal(err)
	}
	exec.StorageClient = fc

	defer exec.StorageClient.Close()
	saver := newTestSaver()
	th := reproc.NewTaskHandler(exec, []string{"queue-1"}, saver)

	th.AddTask(ctx, "gs://foo/bar/2001/01/01/")

	go th.AddTask(ctx, "gs://foo/bar/2001/01/02/")
	go th.AddTask(ctx, "gs://foo/bar/2001/01/03/")

	start := time.Now()
	for counter.Count() < 9 && time.Now().Before(start.Add(2*time.Second)) {
		time.Sleep(100 * time.Millisecond)
	}

	th.Wait()

	if counter.Count() != 9 {
		t.Error("Wrong number of client calls", counter.Count())
	}
	if len(saver.GetDeletes()) != 3 {
		t.Error("Wrong number of task deletes", len(saver.GetDeletes()))
	}

	tasks := saver.GetTasks()
	for _, task := range tasks {
		// Eventually, final state should be Done.  For now it is an error state
		last := task[len(task)-1]
		if true {
			if last.State != state.Stabilizing {
				t.Error("State should be Stabilizing:", last)
			} else if !strings.Contains(last.ErrMsg, "googleapi: Error 404") {
				t.Error("Bad task state:", task)
			}
		} else {
			if last.State != state.Done {
				t.Error("Bad task state:", task)
			}
		}
	}
}

// By using the "interface" version of the client, we make it possible to sub in
// our own fakes at any level. Here we sub in a fake Client which returns a fake
// BucketHandle that returns a series of fake Objects.
type fakeClient struct {
	stiface.Client
	objects []*storage.ObjectAttrs // Objects that will be returned by iterator
}

func (f fakeClient) Close() error {
	return nil
}

func (f fakeClient) Bucket(name string) stiface.BucketHandle {
	return &fakeBucketHandle{objects: f.objects}
}

type fakeBucketHandle struct {
	stiface.BucketHandle
	objects []*storage.ObjectAttrs // Objects that will be returned by iterator
}

func (bh fakeBucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{}, nil
}

func (bh fakeBucketHandle) Objects(context.Context, *storage.Query) stiface.ObjectIterator {
	n := 0
	return fakeObjectIterator{next: &n, objects: bh.objects}
}

type fakeObjectIterator struct {
	stiface.ObjectIterator
	objects []*storage.ObjectAttrs
	next    *int
}

func (it fakeObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if *it.next >= len(it.objects) {
		return nil, iterator.Done
	}
	log.Println(it.next)
	*it.next += 1
	return it.objects[*it.next-1], nil
}

func TestBadPrefix(t *testing.T) {
	ctx := context.Background()
	client, _ := cloud.DryRunClient()
	config := cloud.Config{Project: "mlab-testing", Client: client}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "bqproject", BQBatchDataset: "dataset"}
	fc := fakeClient{objects: []*storage.ObjectAttrs{
		&storage.ObjectAttrs{Name: "obj1"},
		&storage.ObjectAttrs{Name: "obj2"},
	}}
	exec := &rex.ReprocessingExecutor{BQConfig: bqConfig, StorageClient: fc}
	defer exec.StorageClient.Close()

	saver := newTestSaver()
	th := reproc.NewTaskHandler(exec, []string{"queue-1"}, saver)

	err := th.AddTask(ctx, "gs://foo/bar/badprefix/01/01/")
	if err == nil {
		th.Wait()
		log.Fatal("AddTask should return bad prefix error")
	}

	if !strings.HasPrefix(err.Error(), "Invalid test path:") {
		log.Println("Should have invalid test path error")
		t.Error(err)
	}
}

func TestZeroFiles(t *testing.T) {
	ctx := context.Background()
	client, _ := cloud.DryRunClient()
	config := cloud.Config{Project: "mlab-testing", Client: client}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "bqproject", BQBatchDataset: "dataset"}
	fc := fakeClient{objects: []*storage.ObjectAttrs{}}
	exec := &rex.ReprocessingExecutor{BQConfig: bqConfig, StorageClient: fc}
	defer exec.StorageClient.Close()

	saver := newTestSaver()
	th := reproc.NewTaskHandler(exec, []string{"queue-1"}, saver)

	err := th.AddTask(ctx, "gs://foo/bar/2001/01/01/")
	if err != nil {
		t.Fatal(err)
	}
	th.Wait()

	tasks := saver.GetTasks()
	if len(tasks) != 1 {
		t.Fatal("Should be exactly one task record")
	}
	for _, task := range tasks {
		// Eventually, final state should be Done.  For now it is an error state
		last := task[len(task)-1]
		if last.State != state.Done {
			t.Error("Bad task state:", task)
		}
	}
}
