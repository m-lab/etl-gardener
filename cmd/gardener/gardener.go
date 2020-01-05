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

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"
	"golang.org/x/sync/errgroup"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/httpx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl-gardener/cloud"
	job "github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/reproc"
	"github.com/m-lab/etl-gardener/rex"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/etl-gardener/tracker"

	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

var (
	jobExpirationTime = flag.Duration("job_expiration_time", 4*time.Hour, "Time after which stale jobs will be purged")
	shutdownTimeout   = flag.Duration("shutdown_timeout", 1*time.Minute, "Graceful shutdown time allowance")
	statusPort        = flag.String("status_port", ":0", "The public interface port where status (and pprof) will be published")

	// Context and injected variables to allow smoke testing of main()
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Environment provides "global" variables.
// Any env vars that we want to read only at startup should be stored here.
type environment struct {
	Error   error // Any error encountered during LoadEnv.
	Project string

	// Vars for Status()
	Commit  string
	Release string

	// Determines what kind of service to run.
	// "manager" will cause loading of config, and passive management.
	// "legacy" will cause legacy behavior based on additional environment variables.
	ServiceMode string

	Bucket string

	// These are only used for the "legacy" service mode.
	BatchDataset string // All working data is written to the batch data set.
	FinalDataset string // Ultimate deduplicated, partitioned output dataset.
	QueueBase    string // Base of the queue names.  Will append -0, -1, ...
	Experiment   string
	NumQueues    int
	StartDate    time.Time // The archive date to start reprocessing.
	DateSkip     int       // Number of dates to skip between PostDay, to allow rapid scanning in sandbox.
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

func loadEnvVarsForTaskQueue() {
	var ok bool
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
}

// LoadEnv loads any required environment variables.
func LoadEnv() {
	var ok bool
	env.Commit = os.Getenv("GIT_COMMIT")
	env.Release = os.Getenv("RELEASE_TAG")
	env.BatchDataset = os.Getenv("DATASET")
	env.FinalDataset = os.Getenv("FINAL_DATASET")

	env.Project, ok = os.LookupEnv("PROJECT")
	if !ok {
		env.Error = ErrNoProject
		log.Println(env.Error)
	}

	env.ServiceMode, ok = os.LookupEnv("SERVICE_MODE")
	if !ok {
		env.ServiceMode = "legacy"
	}

	switch env.ServiceMode {
	case "manager":
		bucket := os.Getenv("ARCHIVE_BUCKET")
		if bucket == "" {
			log.Println("Error: ARCHIVE_BUCKET environment variable not set.")
			env.Error = ErrNoBucket
		} else {
			env.Bucket = bucket
		}
	case "legacy":
		// load variables required for task queue based operation.
		loadEnvVarsForTaskQueue()
	}
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

// StartDateRFC3339 is the date at which reprocessing will start when it catches
// up to present.  For now, we are making this the beginning of the ETL timeframe,
// until we get annotation fixed to use the actual data date instead of NOW.
const StartDateRFC3339 = "2017-05-01T00:00:00Z"

// Job state tracker, when operating in manager mode.
var globalTracker *tracker.Tracker

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
	if globalTracker != nil {
		globalTracker.WriteHTMLStatusTo(r.Context(), w)
	}
	state.WriteHTMLStatusTo(r.Context(), w, env.Project, env.Experiment)
	fmt.Fprintf(w, "</br>\n")

	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

// Used for testing.
var statusServerAddr string

func startStatusServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)

	// Also allow pprof through the status server.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Start up the http server.
	server := &http.Server{
		Addr:    *statusPort,
		Handler: mux,
	}
	rtx.Must(httpx.ListenAndServeAsync(server), "Could not start status server")

	statusServerAddr = server.Addr

	return server
}

var healthy = false

// healthCheck, for now, used for both /ready and /alive.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	if !healthy {
		log.Println("Reporting unhealthy for", r.RequestURI)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Internal server error."}`)
	} else {
		fmt.Fprint(w, "ok")
	}
}

func mustStandardTracker() *tracker.Tracker {
	client, err := datastore.NewClient(context.Background(), env.Project)
	rtx.Must(err, "datastore client")
	dsKey := datastore.NameKey("tracker", "jobs", nil)
	dsKey.Namespace = "gardener"

	tk, err := tracker.InitTracker(
		context.Background(),
		dsiface.AdaptClient(client), dsKey,
		time.Minute, *jobExpirationTime)
	rtx.Must(err, "tracker init")
	if tk == nil {
		log.Fatal("nil tracker")
	}

	return tk
}

// ###############################################################################
//  Main
// ###############################################################################

func main() {
	defer mainCancel()

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	LoadEnv()
	if env.Error != nil {
		log.Println(env.Error)
		log.Println(env)
		os.Exit(1)
	}

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	// Expose prometheus and pprof metrics on a separate port.
	promServer := prometheusx.MustServeMetrics()
	defer promServer.Close()

	statusServer := startStatusServer()
	defer statusServer.Close()
	log.Println("Status server at", statusServer.Addr)

	mux := http.NewServeMux()
	// Start up the main job and update server.
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)

	// TODO - do we want different health checks for manager mode?
	mux.HandleFunc("/alive", healthCheck)
	mux.HandleFunc("/ready", healthCheck)

	switch env.ServiceMode {
	case "manager":
		// This is new new "manager" mode, in which Gardener provides /job and /update apis
		// for parsers to get work and report progress.
		globalTracker = mustStandardTracker()
		handler := tracker.NewHandler(globalTracker)
		handler.Register(mux)

		// For now, we just start in Aug 2019, and handle only new data.
		svc, err := job.NewJobService(globalTracker, time.Date(2019, 8, 1, 0, 0, 0, 0, time.UTC))
		rtx.Must(err, "Could not initialize job service")
		mux.HandleFunc("/job", svc.JobHandler)
		healthy = true
		log.Println("Running as manager service")
	case "legacy":
		// This is the "legacy" mode, that manages work through task queues.
		// TODO - this creates a storage client, which should be closed on termination.
		th, err := taskHandlerFromEnv(mainCtx, http.DefaultClient)

		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// If RunDispatchLoop() returns an err instead of nil, healthy will be
		// set as false and eventually it will cause kubernetes to roll back.
		err = reproc.RunDispatchLoop(mainCtx, th, env.Project, env.Bucket, env.Experiment, env.StartDate, env.DateSkip)
		if err != nil {
			healthy = false
			log.Println("Running as unhealthy service")
		} else {
			healthy = true
			log.Println("Running as task-queue dispatcher service")
		}
	default:
		log.Println("Unrecognized SERVICE_MODE.  Expected manager or legacy")
		os.Exit(1)
	}

	rtx.Must(httpx.ListenAndServeAsync(server), "Could not start main server")

	select {
	case <-mainCtx.Done():
		log.Println("Shutting down servers")
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		start := time.Now()
		eg := errgroup.Group{}
		eg.Go(func() error {
			return server.Shutdown(ctx)
		})
		eg.Go(func() error {
			return statusServer.Shutdown(ctx)
		})
		eg.Go(func() error {
			return promServer.Shutdown(ctx)
		})
		eg.Wait()
		log.Println("Shutdown took", time.Since(start))
	}
}
