// Package api contains interfaces used in the project.
package api

import "github.com/m-lab/etl-gardener/state"

// BasicPipe specifies an interface for sending jobs to a downstream handler.
type BasicPipe interface {
	Sink() chan<- state.Task
	Response() <-chan error
}

// NilBasicPipe is the trivial implementation of BasicPipe
type NilBasicPipe struct{}

// Sink returns the sink channel, for use by the sender.
func (nd *NilBasicPipe) Sink() chan<- state.Task {
	return nil
}

// Response returns the response channel, that closes when all processing is complete.
func (nd *NilBasicPipe) Response() <-chan error {
	return nil
}

func assertBasicPipe(ds BasicPipe) {
	func(ds BasicPipe) {}(&NilBasicPipe{})
}
