package tracker_test

import (
	"testing"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestStatusUpdate(t *testing.T) {
	s := tracker.NewStatus()
	s.Update(tracker.Parsing, "init done")
	if s.History[0].LastUpdate != "init done" {
		t.Error(s.History[0])
	}
	s.UpdateDetail("Still parsing")
	if s.LastUpdate() != "Still parsing" {
		t.Error(s.LastUpdate())
	}

	// Each Update with new State should result in empty LastUpdate field.
	s.Update(tracker.Deduplicating, "Parsing complete")
	if s.LastUpdate() != "" {
		t.Error(s.LastUpdate())
	}

	s.Update(tracker.Complete, "Dedup took xxx")
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	last := s.LastStateInfo()
	if last.LastUpdate != "" {
		t.Error(last)
	}
	t.Log(s.LastUpdate())
}

func TestStatusFailure(t *testing.T) {
	s := tracker.NewStatus()
	s.Update(tracker.Parsing, "init done")
	if s.History[0].LastUpdate != "init done" {
		t.Error(s.History[0])
	}
	s.UpdateDetail("Still parsing")
	if s.LastUpdate() != "Still parsing" {
		t.Error(s.LastUpdate())
	}

	// Each Update with new State should result in empty LastUpdate field.
	s.Update(tracker.Deduplicating, "Parsing complete")
	if s.LastUpdate() != "" {
		t.Error(s.LastUpdate())
	}

	s.Update(tracker.Failed, "failed")
	if len(s.History) != 4 {
		t.Error("length =", len(s.History))
	}
	last := s.LastStateInfo()
	if last.LastUpdate != "failed" {
		t.Error(last)
	}
	t.Log(s.LastUpdate())
}
