package cloud

import (
	"net/http"

	"google.golang.org/api/option"
)

// Config provides a generic config suitable for many cloud clients.
type Config struct {
	Project string
	Dataset string // TODO - do we want this here?  Only used for bigquery
	Client  *http.Client
	Options []option.ClientOption

	TestMode bool
}
