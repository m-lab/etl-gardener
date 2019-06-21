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
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/go/prometheusx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"

	"github.com/m-lab/etl-gardener/state"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

// Environment provides "global" variables.
// Any env vars that we want to read only at startup should be stored here.
type environment struct {
	// Vars for newDispatcher
	Error        error
	Project      string
	BatchDataset string // All working data is written to the batch data set.
	FinalDataset string // Ultimate deduplicated, partitioned output dataset.
	QueueBase    string // Base of the queue names.  Will append -0, -1, ...
	Experiment   string
	Bucket       string
	NumQueues    int
	StartDate    time.Time // The archive date to start reprocessing.
	DateSkip     int       // Number of dates to skip between PostDay, to allow rapid scanning in sandbox.

	// Vars for Status()
	Commit  string
	Release string

	TestMode bool
}

// env provides environment vars.
var env environment

// Errors associated with environment.
var (
	ErrNoProject       = errors.New("No env var for Project")
	ErrNoQueueBase     = errors.New("No env var for QueueBase")
	ErrNoNumQueues     = errors.New("No env var for NumQueues")
	ErrNoStartDate     = errors.New("No env var for StartDate")
	ErrInvalidDateSkip = errors.New("Invalid DATE_SKIP value")
	ErrBadStartDate    = errors.New("Bad StartDate")
	ErrNoExperiment    = errors.New("No env var for Experiment")
	ErrNoBucket        = errors.New("No env var for Bucket")
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

	skipCountString := os.Getenv("DATE_SKIP")
	if skipCountString != "" {
		env.DateSkip, err = strconv.Atoi(skipCountString)
		if err != nil {
			log.Println(err)
			env.Error = ErrInvalidDateSkip
			log.Println(env.Error)
		}
	}

	expt := os.Getenv("EXPERIMENT")
	if expt == "" {
		env.Error = ErrNoExperiment
		log.Println("Error: EXPERIMENT environment variable not set.")
	} else {
		env.Experiment = strings.TrimSpace(expt)
	}

	bucket := os.Getenv("TASKFILE_BUCKET")
	if bucket == "" {
		log.Println("Error: TASKFILE_BUCKET environment variable not set.")
		env.Error = ErrNoBucket
	} else {
		env.Bucket = bucket
	}

	env.Commit = os.Getenv("GIT_COMMIT")
	env.Release = os.Getenv("RELEASE_TAG")
	env.BatchDataset = os.Getenv("DATASET")
	env.FinalDataset = os.Getenv("FINAL_DATASET")
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
	return cloud.BQConfig{
		Config:         config,
		BQProject:      config.Project,
		BQBatchDataset: env.BatchDataset,
		BQFinalDataset: env.FinalDataset,
	}
}

// dispatcherFromEnv creates a Dispatcher struct initialized from environment variables.
// It uses PROJECT, QUEUE_BASE, and NUM_QUEUES.
// NOTE: ctx should only be used within the function scope, and not reused later.
// Not currently clear if that is true.
func taskHandlerFromEnv(ctx context.Context, client *http.Client) (*reproc.TaskHandler, error) {
	if env.Error != nil {
		log.Println(env.Error)
		log.Println(env)
		return nil, env.Error
	}

	config := cloud.Config{
		Project: env.Project,
		Client:  client}

	bqConfig := NewBQConfig(config)
	exec, err := rex.NewReprocessingExecutor(ctx, bqConfig)
	if err != nil {
		return nil, err
	}
	// TODO - exec.StorageClient should be closed.
	queues := make([]string, env.NumQueues)
	for i := 0; i < env.NumQueues; i++ {
		queues[i] = fmt.Sprintf("%s%d", env.QueueBase, i)
	}

	// TODO move DatastoreSaver to another package?
	saver, err := state.NewDatastoreSaver(ctx, env.Project)
	if err != nil {
		return nil, err
	}
	return reproc.NewTaskHandler(env.Experiment, exec, queues, saver), nil
}

// doDispatchLoop just sequences through archives in date order.
// It will generally be blocked on the queues.
// It will start processing at startDate, and when it catches up to "now" it will restart at restartDate.
func doDispatchLoop(ctx context.Context, handler *reproc.TaskHandler, startDate time.Time, restartDate time.Time, bucket string, experiment string) {
	log.Println("(Re)starting at", startDate)
	next := startDate

	for {
		prefix := next.Format(fmt.Sprintf("gs://%s/%s/2006/01/02/", bucket, experiment))

		// Note that this blocks until a queue is available.
		err := handler.AddTask(ctx, prefix)
		if err != nil {
			// Only error expected here is ErrTerminating
			log.Println(err)
			return
		}

		// Advance to next date, possibly skipping days if DATE_SKIP env var was set.
		next = next.AddDate(0, 0, 1+env.DateSkip)

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

	// TODO - attach the environment to the context.
	state.WriteHTMLStatusTo(r.Context(), w, env.Project, env.Experiment)
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

// setupService prepares the setting for a service.
// The configuration info comes from environment variables.
func setupService(ctx context.Context) error {
	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	// Expose prometheus and pprof metrics on a separate port.
	prometheusx.MustStartPrometheus(":9090")

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)

	http.HandleFunc("/alive", healthCheck)
	http.HandleFunc("/ready", healthCheck)

	// TODO - this creates a storage client, which should be closed on termination.
	handler, err := taskHandlerFromEnv(ctx, http.DefaultClient)

	if err != nil {
		log.Println(err)
		return err
	}

	startDate := env.StartDate

	ds, err := state.NewDatastoreSaver(ctx, env.Project)
	if err != nil {
		log.Println(err)
		return err
	}

	// Move the timeout into GetStatus?
	taskCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	tasks, err := ds.GetStatus(taskCtx, env.Experiment)
	cancel()

	if err != nil {
		log.Println(err)
		return err
	}
	maxDate, err := handler.RestartTasks(ctx, tasks)
	if err != nil {
		log.Println(err)
		return err
	}

	// Move the start date after the max observed date.
	// Note that if we restart while wrapping back to start date, this will essentially
	// result in restarting at the original start date, after wrapping.
	for !maxDate.Before(startDate) {
		startDate = startDate.AddDate(0, 0, 1+env.DateSkip)
	}

	log.Println("Using start date of", startDate)
	go doDispatchLoop(ctx, handler, startDate, env.StartDate, env.Bucket, env.Experiment)

	return nil
}

// ###############################################################################
//  Main
// ###############################################################################

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check if invoked as a service.
	isService, _ := strconv.ParseBool(os.Getenv("GARDENER_SERVICE"))
	if isService {
		// If setupService() returns an err instead of nil, healthy will be
		// set as false and eventually it will cause kubernetes to roll back.
		err := setupService(ctx)
		if err != nil {
			healthy = false
			log.Println("Running as unhealthy service")
		} else {
			healthy = true
			log.Println("Running as service")
		}
		log.Fatal(http.ListenAndServe(":8080", nil))
		return
	}

	// Otherwise this is a command line invocation...
	// TODO add implementation (see code in etl repo)
	log.Println("Command line not implemented")
}
