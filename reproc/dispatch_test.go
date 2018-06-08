package reproc_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/state"
)

func assertTaskPipe(t state.Terminator) {
	func(t state.Terminator) {}(&reproc.Terminator{})
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

// This test exercises the task management, including invoking t.Process().
//  It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestBasic(t *testing.T) {
	// Start tracker with no queues.
	th := reproc.NewTaskHandler([]string{})

	// This will block because there are no queues.
	go th.AddTask("foobar")
	// Just so it is clear where the message comes from...
	time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)

	th.Terminate()
	th.Wait() // Race
}

// This test exercises the task management, including invoking t.Process().
//  It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
func TestWithTaskQueue(t *testing.T) {
	// Start tracker with no queues.
	th := reproc.NewTaskHandler([]string{"queue-1"})

	th.AddTask("a")

	go th.AddTask("b")
	go th.AddTask("c")

	time.Sleep(15 * time.Millisecond)
	th.Terminate()
	th.Wait()
}
