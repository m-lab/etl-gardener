package tracker_test

import (
	"testing"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestStatusUpdate(t *testing.T) {
	s := tracker.NewStatus()
	s.UpdateDetail("done")
	s.NewState(tracker.Parsing)
	s.UpdateDetail("parsing")
	if s.LastUpdate() != "parsing" {
		t.Error(s.LastUpdate())
	}
	if s.History[0].LastUpdate != "done" {
		t.Error(s.History[0])
	}

	s.UpdateDetail("Parsing complete")
	// NewState should still return old LastUpdate.
	s.NewState(tracker.Deduplicating)
	if s.LastUpdate() != "Parsing complete" {
		t.Error(s.LastUpdate())
	}

	s.UpdateDetail("Dedup took xxx")
	s.NewState(tracker.Complete)
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	last := s.LastStateInfo()
	// LastStateInfo's update should be empty.
	if last.LastUpdate != "" {
		t.Error(last)
	}
	t.Log(s.LastUpdate())
}

func TestStatusFailure(t *testing.T) {
	s := tracker.NewStatus()
	s.UpdateDetail("done")
	s.NewState(tracker.Parsing)
	s.UpdateDetail("parsing")
	if s.LastUpdate() != "parsing" {
		t.Error(s.LastUpdate())
	}
	// Original detail unchanged...
	if s.History[0].LastUpdate != "done" {
		t.Error(s.History[0])
	}

	// NewState should still return old LastUpdate.
	s.NewState(tracker.Deduplicating)
	// Should still get old update.
	if s.LastUpdate() != "parsing" {
		t.Error(s.LastUpdate())
	}

	s.NewState(tracker.Failed)
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	// LastStateInfo should have empty update.
	last := s.LastStateInfo()
	if last.LastUpdate != "" {
		t.Error(last)
	}
	t.Log(s.LastUpdate())
}
