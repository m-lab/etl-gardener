package reproc_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/reproc"
)

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
	tt, err := reproc.StartTaskTracker([]string{})
	if err != nil {
		t.Fatal(err)
	}

	// This will block because there are no queues.
	go tt.AddTask("foobar")
	// Just so it is clear where the message comes from...
	time.Sleep(10 * time.Millisecond)

	tt.Terminate()
	tt.Wait() // Race
}

func TestWithTaskQueue(t *testing.T) {
	// Start tracker with no queues.
	tt, err := reproc.StartTaskTracker([]string{"queue-1"})
	if err != nil {
		t.Fatal(err)
	}

	tt.AddTask("foo")

	go tt.AddTask("bar")

	var bb []byte
	buf := bytes.NewBuffer(bb)
	tt.GetStatus(buf)
	if string(buf.Bytes()) != "status..." {
		t.Error("Expected status...", string(buf.Bytes()))
	}

	tt.Terminate()
	tt.Wait()
}
