package jobtest

import (
	"time"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/tracker"
)

// NewJob creates a new tracker.Job for unit testing.
func NewJob(bucket, exp, typ string, date time.Time) tracker.Job {
	return tracker.Job{
		Bucket:     bucket,
		Experiment: exp,
		Datatype:   typ,
		Date:       date.UTC().Truncate(24 * time.Hour),
		Datasets: config.Datasets{
			Temp: "tmp_" + exp,
			Raw:  "raw_" + exp,
		},
	}
}
