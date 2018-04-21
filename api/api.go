// Package api contains interfaces used in the project.
package api

import "github.com/m-lab/etl-gardener/state"

// TaskPipe specifies an interface for sending jobs to a downstream handler.
type TaskPipe interface {
	Sink() chan<- state.Task
	Response() <-chan error
}

// NilTaskPipe is the trivial implementation of TaskPipe
type NilTaskPipe struct{}

// Sink returns the sink channel, for use by the sender.
func (nd *NilTaskPipe) Sink() chan<- state.Task {
	return nil
}

// Response returns the response channel, that closes when all processing is complete.
func (nd *NilTaskPipe) Response() <-chan error {
	return nil
}

func assertTaskPipe(ds TaskPipe) {
	func(ds TaskPipe) {}(&NilTaskPipe{})
}
