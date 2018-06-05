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

func TestBasic(t *testing.T) {
	// Start tracker with no queues.
	tt := reproc.NewTaskTracker([]string{})

	// This will block because there are no queues.
	go tt.AddTask("foobar")
	// Just so it is clear where the message comes from...
	time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)

	tt.Terminate()
	tt.Wait() // Race
}

func TestWithTaskQueue(t *testing.T) {
	// Start tracker with no queues.
	tt := reproc.NewTaskTracker([]string{"queue-1"})

	tt.AddTask("a")

	go tt.AddTask("b")
	go tt.AddTask("c")

	time.Sleep(15 * time.Millisecond)
	tt.Terminate()
	tt.Wait()
}
