// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartDateRFC3339 is the date at which reprocessing will start when it catches
// up to present.  For now, we are making this the beginning of the ETL timeframe,
// until we get annotation fixed to use the actual data date instead of NOW.
const StartDateRFC3339 = "2017-05-01T00:00:00Z00:00"

// ###############################################################################
//  Batch processing task scheduling and support code
// ###############################################################################

// Persistent Queuer for use in handlers and gardener tasks.
var batchQueuer tq.Queuer

// QueueState holds the state information for each batch queue.
type QueueState struct {
	// Per queue, indicate which day is being processed in
	// that queue.  No need to process more than one day at a time.
	// The other queues will take up the slack while we add more
	// tasks when one queue is emptied.

	QueueName        string // Name of the batch queue.
	NextTask         string // Name of task file currently being enqueued.
	PendingPartition string // FQ Name of next table partition to be added.
}

// MaybeScheduleMoreTasks will look for an empty task queue, and if it finds one, will look
// for corresponding days to add to the queue.
// Alternatively, it may look first for the N oldest days to be reprocessed, and will then
// check whether any of the task queues for those oldest days is empty, and conditionally add tasks.
func MaybeScheduleMoreTasks(queuer *tq.Queuer) {
	// GetTaskQueueDepth returns the number of pending items in a task queue.
	stats, err := queuer.GetTaskqueueStats()
	if err != nil {
		log.Println(err)
	} else {
		for k, v := range stats {
			if v.Tasks == 0 && v.InFlight == 0 {
				log.Printf("Ready: %s: %v\n", k, v)
				// Should add more tasks now.
			}
		}
	}
}

// queuerFromEnv creates a Queuer struct initialized from environment variables.
// It uses TASKFILE_BUCKET, PROJECT, QUEUE_BASE, and NUM_QUEUES.
func queuerFromEnv() (tq.Queuer, error) {
	bucketName, ok := os.LookupEnv("TASKFILE_BUCKET")
	if !ok {
		return tq.Queuer{}, errors.New("TASKFILE_BUCKET not set")
	}
	project, ok := os.LookupEnv("PROJECT")
	if !ok {
		return tq.Queuer{}, errors.New("PROJECT not set")
	}
	queueBase, ok := os.LookupEnv("QUEUE_BASE")
	if !ok {
		return tq.Queuer{}, errors.New("QUEUE_BASE not set")
	}
	numQueues, err := strconv.Atoi(os.Getenv("NUM_QUEUES"))
	if err != nil {
		log.Println(err)
		return tq.Queuer{}, errors.New("Parse error on NUM_QUEUES")
	}

	return tq.CreateQueuer(http.DefaultClient, nil, queueBase, numQueues, project, bucketName, false)
}

// ###############################################################################
//  Top level service control code.
// ###############################################################################

func setupPrometheus() {
	// Define a custom serve mux for prometheus to listen on a separate port.
	// We listen on a separate port so we can forward this port on the host VM.
	// We cannot forward port 8080 because it is used by AppEngine.
	mux := http.NewServeMux()
	// Assign the default prometheus handler to the standard exporter path.
	mux.Handle("/metrics", promhttp.Handler())
	// Assign the pprof handling paths to the external port to access individual
	// instances.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go http.ListenAndServe(":9090", mux)
}

// Status provides basic information about the service.  For now, it is just
// configuration and version info.  In future it will likely include more
// dynamic information.
// TODO(gfr) Add either a black list or a white list for the environment
// variables, so we can hide sensitive vars. https://github.com/m-lab/etl/issues/384
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>NOTE: This is just one of potentially many instances.</p>\n")
	commit := os.Getenv("COMMIT_HASH")
	if len(commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl-gardener/tree/%s\">%s</a><br>\n",
			os.Getenv("RELEASE_TAG"), os.Getenv("COMMIT_HASH"), os.Getenv("COMMIT_HASH")[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s   Commit: unknown\n", os.Getenv("RELEASE_TAG"))
	}

	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

var healthy = false

// healthCheck, for now, used for both /ready and /alive.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	if !healthy {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Internal server error."}`)
	}
	fmt.Fprint(w, "ok")
}

func periodic() {

}

// runService starts a service handler and runs forever.
// The configuration info comes from environment variables.
func runService() {
	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	setupPrometheus()
	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)

	http.HandleFunc("/alive", healthCheck)
	http.HandleFunc("/ready", healthCheck)

	var err error
	batchQueuer, err = queuerFromEnv()
	if err == nil {
		healthy = true
		log.Println("Running as a service.")

		// Run the background "periodic" function.
		go periodic()
	} else {
		// Leaving healthy == false
		// This will cause app-engine to roll back.
		log.Println(err)
		log.Println("Required environment variables are missing or invalid.")
	}

	// ListenAndServe, and terminate when it returns.
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// ###############################################################################
//  Top level command line code.
// ###############################################################################

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// Check if invoked as a service.
	isService, _ := strconv.ParseBool(os.Getenv("GARDENER_SERVICE"))
	if isService {
		runService()
		return
	}
}
