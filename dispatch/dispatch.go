// Package dispatch contains the logic for dispatching new reprocessing tasks.
package dispatch

import (
	"errors"
	"log"
	"os"

	"github.com/m-lab/etl-gardener/cloud"
)

// Dispatcher related errors.
var (
	ErrTerminating = errors.New("dispatcher is terminating")
)

// NewBQConfig creates a BQConfig for use with NewDedupHandler
func NewBQConfig(config cloud.Config) cloud.BQConfig {
	bqDataset, ok := os.LookupEnv("DATASET")
	if !ok {
		log.Println("ERROR: env.DATASET not set")
	}

	// When running in prod, the task files and queues are in mlab-oti, but the destination
	// BigQuery tables are in measurement-lab.
	// However, for sidestream private tables, we leave them in mlab-oti
	bqProject := config.Project
	if bqProject == "mlab-oti" && bqDataset != "private" {
		bqProject = "measurement-lab" // destination for production tables.
	}
	return cloud.BQConfig{Config: config, BQProject: bqProject, BQDataset: bqDataset}
}
