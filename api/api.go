package api

// Downstream specifies an interface for sending jobs to a downstream handler.
type Downstream interface {
	Sink() chan<- string
	Responses() <-chan error
	Done() <-chan bool
}
