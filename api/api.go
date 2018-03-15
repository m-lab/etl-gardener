// Package api contains interfaces used in the project.
package api

// Downstream specifies an interface for sending jobs to a downstream handler.
type Downstream interface {
	Sink() chan<- string
	Response() <-chan error
}

// NilDownstream is the trivial implementation of Downstream
type NilDownstream struct{}

// Sink returns the sink channel, for use by the sender.
func (nd *NilDownstream) Sink() chan<- string {
	return nil
}

// Response returns the response channel, that closes when all processing is complete.
func (nd *NilDownstream) Response() <-chan error {
	return nil
}

func assertDownstream(ds Downstream) {
	assertDownstream(&NilDownstream{})
}
