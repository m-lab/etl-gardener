// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/etl-gardener/dispatch"
	"github.com/m-lab/etl-gardener/state"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

// Environment provides "global" variables.
// Any env vars that we want to read only at startup should be stored here.
type environment struct {
	// Vars for newDispatcher
	Error     error
	Project   string
	QueueBase string
	NumQueues int
	StartDate time.Time

	// Vars for Status()
	Commit  string
	Release string

	TestMode bool
}

// env provides environment vars.
var env environment

// Errors associated with environment.
var (
	ErrNoProject    = errors.New("No env var for Project")
	ErrNoQueueBase  = errors.New("No env var for QueueBase")
	ErrNoNumQueues  = errors.New("No env var for NumQueues")
	ErrNoStartDate  = errors.New("No env var for StartDate")
	ErrBadStartDate = errors.New("Bad StartDate")
)

// LoadEnv loads any required environment variables.
func LoadEnv() {
	var ok bool
	env.Project, ok = os.LookupEnv("PROJECT")
	if !ok {
		env.Error = ErrNoProject
		log.Println(env.Error)
	}
	env.QueueBase, ok = os.LookupEnv("QUEUE_BASE")
	if !ok {
		env.Error = ErrNoQueueBase
		log.Println(env.Error)
	}
	var err error
	env.NumQueues, err = strconv.Atoi(os.Getenv("NUM_QUEUES"))
	if err != nil {
		env.Error = ErrNoNumQueues
		log.Println(err)
		log.Println(env.Error)
	}
	startString, ok := os.LookupEnv("START_DATE")
	if !ok {
		env.Error = ErrNoStartDate
		log.Println(env.Error)
	} else {
		env.StartDate, err = time.Parse("20060102", startString)
		if !ok {
			env.Error = ErrBadStartDate
			log.Println(env.Error)
		}
	}

	env.Commit = os.Getenv("GIT_COMMIT")
	env.Release = os.Getenv("RELEASE_TAG")
}

func init() {
	// HACK This allows some modified behavior when running unit tests.
	if flag.Lookup("test.v") != nil {
		env.TestMode = true
	}
	LoadEnv()
}

// ###############################################################################
//  Batch processing task scheduling and support code
// ###############################################################################

// dispatcherFromEnv creates a Dispatcher struct initialized from environment variables.
// It uses PROJECT, QUEUE_BASE, and NUM_QUEUES.
func dispatcherFromEnv(client *http.Client) (*dispatch.Dispatcher, error) {
	if env.Error != nil {
		log.Println(env.Error)
		log.Println(env)
		return nil, env.Error
	}
	ds, err := state.NewDatastoreSaver()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return dispatch.NewDispatcher(client, env.Project, env.QueueBase, env.NumQueues, env.StartDate, ds)
}

// StartDateRFC3339 is the date at which reprocessing will start when it catches
// up to present.  For now, we are making this the beginning of the ETL timeframe,
// until we get annotation fixed to use the actual data date instead of NOW.
const StartDateRFC3339 = "2017-05-01T00:00:00Z"

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
	if len(env.Commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl-gardener/tree/%s\">%s</a><br>\n",
			env.Release, env.Commit, env.Commit[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s <br>  Commit: unknown\n", env.Release)
	}

	fmt.Fprintf(w, "</br></br>\n")
	state.WriteHTMLStatusTo(w)
	fmt.Fprintf(w, "</br>\n")

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

	disp, err := dispatcherFromEnv(http.DefaultClient)
	if err != nil {
		log.Println(err)
		// leaving healthy = false should eventually lead to rollback.
	} else {
		// TODO - add termination channel.
		bucket := os.Getenv("TASKFILE_BUCKET")
		expString := os.Getenv("EXPERIMENTS")
		if bucket == "" {
			log.Println("Error: TASKFILE_BUCKET environment variable not set.")
		} else if expString == "" {
			log.Println("Error: EXPERIMENTS environment variable not set.")
		} else {
			experiments := strings.Split(expString, ",")
			for i := range experiments {
				experiments[i] = strings.TrimSpace(experiments[i])
			}
			go disp.DoDispatchLoop(bucket, experiments)
			healthy = true
		}
	}

	// ListenAndServe, and terminate when it returns.
	log.Println("Running as service")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// ###############################################################################
//  Main
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

	// Otherwise this is a command line invocation...
	// TODO add implementation (see code in etl repo)
	log.Println("Command line not implemented")
}
