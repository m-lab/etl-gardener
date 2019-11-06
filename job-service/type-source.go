// Package job provides components to serve up jobs to ETL parsers.
package job

import (
	"errors"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
)

// TypeSource provides a source for prefixes for a particular data type.
type TypeSource struct {
	bucket     string
	expAndType string // like ndt.tcpinfo or ndt or ndt.ndt5
	dateFilter string // optional RegExp to filter dates
	fileFilter string // optional RegExp to filter filenames

	lastJob string                 // name of the last job dispatched to GetNext()
	it      stiface.ObjectIterator // interator for additional objects
}

// ErrNoIterator is returned when a TypeSource has a nil iterator.
var ErrNoIterator = errors.New("TypeSource has no iterator")

// Next returns the next object attributes, or error.
func (ts *TypeSource) Next() (*storage.ObjectAttrs, error) {
	if ts.it == nil {
		return nil, ErrNoIterator
	}
	return ts.it.Next()
}
