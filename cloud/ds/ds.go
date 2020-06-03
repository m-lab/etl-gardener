package ds

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"
)

// Context holds a context for datastore operations
type Context struct {
	Ctx    context.Context
	Client dsiface.Client
	Key    *datastore.Key
}
