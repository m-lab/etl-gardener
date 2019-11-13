package tracker_test

import (
	"context"
	"sync"

	"github.com/m-lab/etl-gardener/tracker"

	"github.com/m-lab/etl-gardener/persistence"
)

type JobState = tracker.JobState

// TODO - generalize to StateObject and move to the persistence package?
type testSaver struct {
	lock  sync.Mutex
	tasks map[string][]tracker.JobState
	// deletes are represented as an empty Task in the tasks sequence.
}

func NewTestSaver() *testSaver {
	return &testSaver{tasks: make(map[string][]JobState, 20)}
}

func (s *testSaver) Save(ctx context.Context, o persistence.StateObject) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	name := o.GetName()
	s.tasks[name] = append(s.tasks[name], *o.(*JobState))
	return nil
}

func (s *testSaver) Delete(ctx context.Context, o persistence.StateObject) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	name := o.GetName()
	s.tasks[name] = append(s.tasks[name], JobState{})
	return nil
}

// This is not quite correct.
func (s *testSaver) Fetch(ctx context.Context, o persistence.StateObject) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	taskStates := s.tasks[o.GetName()]
	if taskStates == nil {
		return tracker.ErrJobNotFound
	}

	last := taskStates[len(taskStates)-1]
	if last.Name == "" {
		return tracker.ErrJobNotFound
	}
	*(o.(*tracker.JobState)) = last
	return nil
}

func (s *testSaver) GetTasks() map[string][]JobState {
	s.lock.Lock()
	defer s.lock.Unlock()
	m := make(map[string][]JobState, len(s.tasks))
	for k, v := range s.tasks {
		m[k] = v
	}
	return m
}

func (s *testSaver) GetTask(name string) []JobState {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.tasks[name]
}

func assertPersistentStore() { func(ex persistence.Saver) {}(&testSaver{}) }
