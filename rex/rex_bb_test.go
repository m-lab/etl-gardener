// +build integration

package rex_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"
)

// This test exercises the task management, including invoking t.Process().
// It does not check any state, but if the termination does not work properly,
// may fail to complete.  Also, running with -race may detect race
// conditions.
// TODO - Consider creating fake BQ tables, so that the dedup phase completes.
func TestRealBucket(t *testing.T) {
	ctx := context.Background()
	client, counter := cloud.DryRunClient()
	config := cloud.Config{Context: ctx, Project: "mlab-testing", Client: client}
	bqConfig := cloud.BQConfig{Config: config, BQProject: "mlab-testing", BQBatchDataset: "batch"}
	exec := rex.ReprocessingExecutor{BQConfig: bqConfig}
	saver := newTestSaver()
	th := reproc.NewTaskHandler(&exec, []string{"queue-1"}, saver)

	// We submit tasks corresponding to real buckets...
	th.AddTask("gs://archive-mlab-testing/ndt/2017/09/22/")

	go th.AddTask("gs://archive-mlab-testing/ndt/2017/09/24/")
	go th.AddTask("gs://archive-mlab-testing/ndt/2017/09/26/")

	// But the jobs will eventually fail because there is no actual task queue,
	// so there won't actually be any templated tables, so BQ dedup will fail.

	// We wait for all three tasks to show up in the saver.
	for counter.Count() < 3 {
		time.Sleep(100 * time.Millisecond)
	}

	for len(saver.GetTasks()) < 3 {
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all tasks to terminate.  The tasks will each use queue-1, and terminate
	// with an error when they reach Stabilizing state.
	th.Wait()

	for _, tk := range saver.GetTasks() {
		log.Println(len(tk), tk[len(tk)-1])
		if len(tk) != 4 {
			t.Error("Incorrect number of updates")
		}
	}

	if counter.Count() != 9 {
		t.Error("Expected 9 client calls:", counter.Count())
	}
	for _, req := range counter.Requests() {
		log.Println(req.URL)
	}

}
