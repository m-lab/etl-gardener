package tracker_test

import (
	"testing"

	"github.com/m-lab/etl-gardener/tracker"
)

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
