// +build integration

package tracker_test

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

// This test assumes the datastore emulator is running.
func TestWithDatastoreSaver(t *testing.T) {
	ctx := context.Background()
	ds, err := persistence.NewDatastoreSaver(ctx, "emulator")
	must(t, err)

	o := tracker.NewJobState("foobar")
	must(t, ds.Save(ctx, &o))

	must(t, ds.Fetch(ctx, &o))

	must(t, ds.Delete(ctx, &o))

	err = ds.Fetch(ctx, &o)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("Should have errored")
	}
}
