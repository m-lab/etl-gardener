package dispatch_test

import (
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/dispatch"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dispatch.TestMode = true
}

func MLabTestAuth() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithAPIKey(os.Getenv("SERVICE_ACCOUNT_mlab_testing"))
		opts = append(opts, authOpt)
	}
	return opts
}

// TODO - use random saver namespace to avoid collisions between multiple testers.
// TODO - use datastore emulator
func TestOwnerLease_Lease(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	// TODO - should use emulator
	ol1, err := dispatch.NewOwnerLease("instance1", "gardener", "test", MLabTestAuth()...)
	if err != nil {
		t.Fatal(err)
	}
	ol2, err := dispatch.NewOwnerLease("instance2", "gardener", "test", MLabTestAuth()...)
	if err != nil {
		t.Fatal(err)
	}

	err = ol1.Lease(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("instance1 has ownership")

	var ol2err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ol2err = ol2.Lease(10 * time.Second)
		wg.Done()
	}()

	time.Sleep(2 * 2 * time.Second)
	// This should notice the request, and relinquish ownership.
	err = ol1.Renew(2 * time.Second)
	if err != dispatch.ErrOwnershipRequested {
		log.Println(err)
	}

	wg.Wait()
	if ol2err != nil {
		t.Fatal(ol2err)
	}

	err = ol1.Renew(2 * time.Second)
	if err != dispatch.ErrLostLease {
		t.Fatal("Should have lost lease:", err)
	}
	err = ol1.Delete()
	if err != dispatch.ErrNotOwner {
		t.Error(err)
	}
	err = ol2.Delete()
	if err != err {
		t.Error(err)
	}
}
