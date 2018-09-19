// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
// Design doc is here: https://docs.google.com/document/d/1503gojY_bVZy1iHlxdDqszADtCt7vFNbT7ZuympRd8A
package main

import (
	"context"
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

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"
	"google.golang.org/api/option"

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
	Dataset   string
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

	env.Dataset = os.Getenv("DATASET")
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

// dispatcherFromEnv creates a Dispatcher struct initialized from environment variables.
// It uses PROJECT, QUEUE_BASE, and NUM_QUEUES.
func taskHandlerFromEnv(client *http.Client) (*reproc.TaskHandler, error) {
	if env.Error != nil {
		log.Println(env.Error)
		log.Println(env)
		return nil, env.Error
	}

	config := cloud.Config{
		Project: env.Project,
		Client:  client}

	bqConfig := NewBQConfig(config)
	exec := rex.ReprocessingExecutor{BQConfig: bqConfig, BucketOpts: []option.ClientOption{}}
	queues := make([]string, env.NumQueues)
	for i := 0; i < env.NumQueues; i++ {
		queues[i] = fmt.Sprintf("%s%d", env.QueueBase, i)
	}

	// TODO move DatastoreSaver to another package?
	saver, err := state.NewDatastoreSaver(env.Project)
	if err != nil {
		return nil, err
	}
	return reproc.NewTaskHandler(&exec, queues, saver), nil
}

// doDispatchLoop just sequences through archives in date order.
// It will generally be blocked on the queues.
// It will start processing at startDate, and when it catches up to "now" it will restart at restartDate.
func doDispatchLoop(handler *reproc.TaskHandler, startDate time.Time, restartDate time.Time, bucket string, experiments []string) {
	log.Println("(Re)starting at", startDate)
	next := startDate

	for {
		for _, e := range experiments {
			prefix := next.Format(fmt.Sprintf("gs://%s/%s/2006/01/02/", bucket, e))

			// Note that this blocks until a queue is available.
			err := handler.AddTask(prefix)
			if err != nil {
				// Only error expected here is ErrTerminating
				log.Println(err)
				return
			}
		}

		next = next.AddDate(0, 0, 1)

		// If gardener has processed all dates up to two days ago,
		// start over.
		if next.Add(48 * time.Hour).After(time.Now()) {
			// TODO - load this from DataStore
			log.Println("Starting over at", restartDate)
			next = restartDate
		}
	}
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
	state.WriteHTMLStatusTo(w, env.Project)
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

	handler, err := taskHandlerFromEnv(http.DefaultClient)
	if err != nil {
		log.Println(err)
		// leaving healthy = false should eventually lead to rollback.
	} else {
		// TODO - add termination channel.
		startDate := env.StartDate
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

			ds, err := state.NewDatastoreSaver(env.Project)
			if err != nil {
				log.Println(err)
				// We just leave healthy = false, and kubernetes should restart.
			} else {
				ctx := context.Background()
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				tasks, err := ds.GetStatus(ctx)
				cancel()
				if err != nil {
					log.Println(err)
				} else {
					maxDate, err := handler.RestartTasks(tasks)
					if err != nil {
						log.Println(err)
					} else {
						if maxDate.After(startDate) {
							startDate = maxDate.AddDate(0, 0, 1)
						}
					}
				}
				log.Println("Using start date of", startDate)
				go doDispatchLoop(handler, startDate, env.StartDate, bucket, experiments)
				healthy = true
			}
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
