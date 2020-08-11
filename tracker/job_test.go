package tracker_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloudtest/gcsfake"
)

func TestPrefixFuncs(t *testing.T) {
	fc := gcsfake.GCSClient{}
	fc.AddTestBucket("fake-bucket",
		gcsfake.BucketHandle{
			ObjAttrs: []*storage.ObjectAttrs{
				{Name: "obj1", Updated: time.Now()},
				{Name: "obj2", Updated: time.Now()},
				{Name: "ndt/ndt5/2011/02/03/foobar.tgz", Size: 101, Updated: time.Now()},
				{Name: "ndt/ndt5/2011/02/03/foobar2.tgz", Size: 2020, Updated: time.Now()},
			}})

	ctx := context.Background()

	ndt5 := tracker.Job{
		Bucket: "fake-bucket", Experiment: "ndt", Datatype: "ndt5", Date: time.Date(2011, 02, 03, 0, 0, 0, 0, time.UTC)}
	if ok, _ := ndt5.HasFiles(ctx, fc); !ok {
		t.Error("Should be ok")
	}
	if files, byteCount, _ := ndt5.PrefixStats(ctx, fc); len(files) != 2 || byteCount != 2121 {
		t.Error("Should have 2 files with 2121 bytes", files, byteCount)
	}

	tcpinfo := tracker.Job{
		Bucket: "fake-bucket", Experiment: "ndt", Datatype: "tcpinfo", Date: time.Date(2011, 02, 03, 0, 0, 0, 0, time.UTC)}
	if ok, _ := tcpinfo.HasFiles(ctx, fc); ok {
		t.Error("HasFiles should be false", ok)
	}
	if files, byteCount, _ := tcpinfo.PrefixStats(ctx, fc); len(files) != 0 || byteCount != 0 {
		t.Error("Should have 0 files, 0 bytes", files, byteCount)
	}
}
func TestStatusUpdate(t *testing.T) {
	s := tracker.NewStatus()
	s.SetDetail("done")
	s.NewState(tracker.Parsing)
	s.SetDetail("parsing")
	if s.Detail() != "parsing" {
		t.Error(s.Detail())
	}
	if s.History[0].Detail != "done" {
		t.Error(s.History[0])
	}

	s.SetDetail("Parsing complete")
	// NewState should still return old Detail.
	s.NewState(tracker.Deduplicating)
	if s.Detail() != "Parsing complete" {
		t.Error(s.Detail())
	}

	s.SetDetail("Dedup took xxx")
	s.NewState(tracker.Complete)
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	last := s.LastStateInfo()
	// LastStateInfo's detail should be empty.
	if last.Detail != "" {
		t.Error(last)
	}
	t.Log(s.Detail())
}

func TestStatusFailure(t *testing.T) {
	s := tracker.NewStatus()
	s.SetDetail("done")
	s.NewState(tracker.Parsing)
	s.SetDetail("parsing")
	if s.Detail() != "parsing" {
		t.Error(s.Detail())
	}
	// Original detail unchanged...
	if s.History[0].Detail != "done" {
		t.Error(s.History[0])
	}

	// NewState should still return old Detail.
	s.NewState(tracker.Deduplicating)
	// Should still get old update.
	if s.Detail() != "parsing" {
		t.Error(s.Detail())
	}

	s.NewState(tracker.Failed)
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	// LastStateInfo should have empty Detail.
	last := s.LastStateInfo()
	if last.Detail != "" {
		t.Error(last)
	}
	t.Log(s.Detail())
}
