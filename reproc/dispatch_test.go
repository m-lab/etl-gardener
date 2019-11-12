package reproc_test

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bou.ke/monkey"

	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/state"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func assertTaskPipe(t state.Terminator) {
	func(t state.Terminator) {}(&reproc.Terminator{})
}

var verbose = log.New(ioutil.Discard, "", 0)

func setVerbose(b bool) {
	if b {
		verbose = log.New(log.Writer(), log.Prefix(), log.Flags())
	} else {
		verbose = log.New(ioutil.Discard, "", 0)
	}
}

// This test exercises the termination sequencing.  It does not check
// any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestTerminator(t *testing.T) {
	trm := reproc.NewTerminator()
	notifier := trm.GetNotifyChannel()

	trm.Add(1)
	go func() {
		<-notifier
		trm.Done()
	}()
	trm.Add(1)
	go func() {
		<-notifier
		trm.Done()
	}()

	trm.Terminate()
	trm.Wait()
}

//===================================================
type testSaver struct {
	tasks  map[string][]state.Task
	delete map[string]struct{}
	lock   sync.Mutex
}

func NewTestSaver() *testSaver {
	return &testSaver{make(map[string][]state.Task, 20), make(map[string]struct{}, 20), sync.Mutex{}}
}

func (s *testSaver) SaveTask(ctx context.Context, t state.Task) error {
	//log.Println(t)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tasks[t.Name] = append(s.tasks[t.Name], t)
	return nil
}

func (s *testSaver) DeleteTask(ctx context.Context, t state.Task) error {
	//log.Println("Delete:", t)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.delete[t.Name] = struct{}{}
	return nil
}

func (s *testSaver) getTask(name string) []state.Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.tasks[name]
}

func (s *testSaver) getTaskStates() [][]state.Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	taskStates := make([][]state.Task, len(s.tasks))
	i := 0
	for _, t := range s.tasks {
		taskStates[i] = t
		i++
	}
	return taskStates
}

func assertPersistentStore() { func(ex state.PersistentStore) {}(&testSaver{}) }

type Exec struct{}

func (ex *Exec) Next(ctx context.Context, t *state.Task, terminate <-chan struct{}) error {
	verbose.Println("Do", t)

	time.Sleep(time.Duration(1+rand.Intn(2)) * time.Millisecond)

	switch t.State {
	case state.Invalid:
		t.Update(ctx, state.Initializing)
	case state.Initializing:
		t.Update(ctx, state.Queuing)
	case state.Queuing:
		t.Update(ctx, state.Processing)
	case state.Processing:
		t.Queue = "" // No longer need to keep the queue.
		t.Update(ctx, state.Stabilizing)
	case state.Stabilizing:
		t.Update(ctx, state.Deduplicating)
	case state.Deduplicating:
		t.Update(ctx, state.Finishing)
	case state.Finishing:
		t.Update(ctx, state.Done)
	case state.Done:
		// Generally shouldn't happen.
		// In prod, we would ignore this, but for test we log.Fatal to force
		// a test failure.
		log.Fatal("Should not call Next when state is Done")
	}
	return nil
}

func AssertExecutor() { func(ex state.Executor) {}(&Exec{}) }

// This test exercises the task management, including invoking t.Process().
//  It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestBasic(t *testing.T) {
	ctx := context.Background()
	// Start tracker with no queues.
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler("exp", &exec, []string{}, saver)

	// This will block because there are no queues.
	go th.AddTask(ctx, "foobar")
	// Just so it is clear where the message comes from...
	time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)

	th.Terminate()
	th.Wait() // Race
}

// This test exercises the task management, including invoking t.Process().
// It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestWithTaskQueue(t *testing.T) {
	setVerbose(true)

	ctx := context.Background()
	// Start tracker with one queue.
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler("exp", &exec, []string{"queue-1"}, saver)

	th.AddTask(ctx, "gs://fake/ndt/2017/09/22/")

	go th.AddTask(ctx, "gs://fake/ndt/2017/09/24/")
	go th.AddTask(ctx, "gs://fake/ndt/2017/09/26/")

	time.Sleep(15 * time.Millisecond)
	th.Terminate()
	th.Wait()
}

func TestRestart(t *testing.T) {
	setVerbose(true)

	ctx := context.Background()
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler("exp", &exec, []string{"queue-1", "queue-2"}, saver)

	taskName := "gs://foobar/exp/2001/02/03/"
	t1, err := state.NewTask("exp", taskName, "queue-1", nil)
	t1.State = state.Processing
	if err != nil {
		t.Fatal(err)
	}
	tasks := []state.Task{*t1}
	th.RestartTasks(ctx, tasks)

	// Restarts are asynchronous, so wait up to 5 seconds for task to be started.
	start := time.Now()
	for time.Since(start) < 5*time.Second &&
		saver.getTask(taskName) == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if saver.getTask(taskName) == nil {
		t.Fatal("Task never started")
	}
}

/*********************************
This block of code vvvvvvvv will move to go/test
***********************************************/

// FakeTime sets the current time to midnight UTC 1 month ago, then advances time at
// the speed indicated by the multiplier.
// NOTE: Since this replaces time.Now() for the entire process, it should not be used in
// parallel, e.g. for concurrent unit tests.
func FakeTime(multiplier int64) func() {
	if flag.Lookup("test.v") == nil {
		log.Fatal("package go/test should not be used outside unit tests")
	}

	var fakeNow int64

	atomic.StoreInt64(&fakeNow, time.Now().AddDate(0, -1, 0).UTC().Truncate(24*time.Hour).UnixNano())

	f := func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&fakeNow)) // race
	}

	monkey.Patch(time.Now, f)

	ticker := time.NewTicker(time.Millisecond)
	go func() {
		for range ticker.C {
			atomic.AddInt64(&fakeNow, multiplier*int64(time.Millisecond))
		}
	}()

	return ticker.Stop
}

// StopFakeTime restores the normal time.Now() function.
func StopFakeTime(stop func()) {
	log.Println("Stopping fake clock")
	stop()
	monkey.Unpatch(time.Now)
}

/*********************************
This block of code ^^^^^^ will move to go/test
***********************************************/

func TestDoDispatchLoop(t *testing.T) {
	setVerbose(false)

	// Set up time to go at approximately 30 days/second.
	stop := FakeTime(int64((30 * 24 * time.Hour) / (1000 * time.Millisecond)))
	// Virtual start time.
	start := time.Now().UTC()

	ctx := context.Background()
	exec := Exec{}
	saver := NewTestSaver()
	th := reproc.NewTaskHandler("exp", &exec, []string{"queue-1", "queue-2", "queue-3"}, saver)

	restart := time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC)
	startDate := time.Date(2013, 2, 1, 0, 0, 0, 0, time.UTC)
	go reproc.DoDispatchLoop(ctx, th, "foobar", "exp", restart, startDate, 0)

	// run for 3 virtual days
	for {
		if time.Since(start) > (3*24+12)*time.Hour {
			break
		}
	}

	//	th.Terminate()

	StopFakeTime(stop)

	// FakeTime starts at midnight UTC, so we should see the previous day.
	recent := "gs://foobar/exp" + start.Add(-24*time.Hour).Format("/2006/01/02/")
	// We expect to see at least 3 distinct recent dates...
	recents := map[string]bool{}
	tasks := saver.getTaskStates()
	for _, task := range tasks {
		taskEnd := task[len(task)-1]
		if taskEnd.Name >= recent {
			t.Log(taskEnd)
			recents[taskEnd.Name] = true
		}
	}

	// Count should be 3 or 4 days.  There is some variation because of the randomness
	// in processing time in the fake Exec.Next() function.
	if len(recents) < 3 {
		t.Error("Should have seen at least 3 daily jobs", recents)
	}
}
