// Package main defines a service for handling various post-processing
// and house-keeping tasks associated with the pipelines.
// Most tasks will be run periodically, but some may be triggered
// by URL requests from authorized sources.
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
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"golang.org/x/sync/errgroup"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/httpx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/config"
	job "github.com/m-lab/etl-gardener/job-service"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/ops"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/state"
	"github.com/m-lab/etl-gardener/tracker"

	// Enable exported debug vars.  See https://golang.org/pkg/expvar/
	_ "expvar"
)

var (
	// GitCommit and Version hold the git commit id and git tags of this build.
	// It is recommended that the strings be set as part of the build/link
	// process, using commands like:
	//
	//   version="-X main.Version=$(git describe --tags)"
	//   commit="-X main.GitCommit=$(git log -1 --format=%H)"
	//   go build -ldflags "$version $commit" ./cmd/gardener
	GitCommit = "nocommit"
	Version   = "noversion"
)

var (
	saverType = flagx.Enum{
		Options: []string{"datastore", "local"},
		Value:   "datastore",
	}
	saverDir string
	project  string

	jobExpirationTime = flag.Duration("job_expiration_time", 24*time.Hour, "Time after which stale jobs will be purged")
	jobCleanupDelay   = flag.Duration("job_cleanup_delay", 3*time.Hour, "Time after which completed jobs will be removed from tracker")
	shutdownTimeout   = flag.Duration("shutdown_timeout", 1*time.Minute, "Graceful shutdown time allowance")
	statusPort        = flag.String("status_port", ":0", "The public interface port where status (and pprof) will be published")
	gardenerAddr      = flag.String("gardener_addr", ":8080", "The listen address for the gardener jobs service")

	// Context and injected variables to allow smoke testing of main()
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Var(&saverType, "saver.backend", "Set the saver backend to 'datastore' or 'local' file.")
	flag.StringVar(&saverDir, "saver.dir", "local", "When the saver backend is 'local', place files in this directory")

	flag.StringVar(&project, "project", "", "GCP project id")
}

// Environment provides "global" variables.
// Any env vars that we want to read only at startup should be stored here.
type environment struct {
	Error   error // Any error encountered during LoadEnv.
	Project string

	// Vars for Status()
	Commit  string
	Version string
}

// env provides environment vars.
var env environment

// Errors associated with environment.
var (
	ErrNoProject = errors.New("no env var for Project")
)

// LoadEnv loads any required environment variables.
func LoadEnv() {
	env.Commit = GitCommit
	env.Version = Version
	env.Project = project
	if env.Project == "" {
		env.Error = ErrNoProject
		log.Println(env.Error)
	}
}

// ###############################################################################
//  Batch processing task scheduling and support code
// ###############################################################################

// NewBQConfig creates a BQConfig for use with NewDedupHandler
func NewBQConfig(config cloud.Config) cloud.BQConfig {
	return cloud.BQConfig{
		Config:    config,
		BQProject: config.Project,
		// BQBatchDataset: env.BatchDataset,
		// BQFinalDataset: env.FinalDataset,
	}
}

/*
TODO: remove
	err = reproc.RunDispatchLoop(mainCtx, th, env.Project, env.Bucket, env.Experiment, env.StartDate, env.DateSkip)
	exec, err := rex.NewReprocessingExecutor(ctx, bqConfig)
	return reproc.NewTaskHandler(env.Experiment, exec, queues, saver), nil
*/

// Job state tracker, when operating in manager mode.
var globalTracker *tracker.Tracker

// ###############################################################################
//  Top level service control code.
// ###############################################################################

// Status provides basic information about the service.  For now, it is just
// configuration and version info.  In future it will likely include more
// dynamic information.
// TODO(github.com/m-lab/etl/issues/1095) Place all public accessible ports behind oauth access.
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	if len(env.Commit) >= 8 {
		fmt.Fprintf(w, "Version: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl-gardener/tree/%s\">%s</a><br>\n",
			env.Version, env.Commit, env.Commit[0:7])
	} else {
		fmt.Fprintf(w, "Version: %s <br>  Commit: unknown\n", env.Version)
	}

	fmt.Fprintf(w, "</br></br>\n")

	// TODO - attach the environment to the context.
	if globalTracker != nil {
		globalTracker.WriteHTMLStatusTo(r.Context(), w)
	}
	state.WriteHTMLStatusTo(r.Context(), w, env.Project)
	fmt.Fprintf(w, "</br>\n")

	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

// Used for testing.
var statusServerAddr string

// Setup ONLY status server, to allow easy access to status
// with minimal security exposure (e.g. no pprof).
func startStatusServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)

	// Start up the http server.
	server := &http.Server{
		Addr:    *statusPort,
		Handler: mux,
	}
	rtx.Must(httpx.ListenAndServeAsync(server), "Could not start status server")

	statusServerAddr = server.Addr

	return server
}

var isReady = false

// healthCheck, for now, used for both /ready and /alive.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	if !isReady {
		log.Println("Reporting unhealthy for", r.RequestURI)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message": "Internal server error."}`)
	} else {
		fmt.Fprint(w, "ok")
	}
}

func mustStandardTracker() *tracker.Tracker {
	var saver tracker.Saver
	dsKey := datastore.NameKey("tracker", "jobs", nil)
	dsKey.Namespace = "gardener"

	switch saverType.Value {
	case "datastore":
		client, err := datastore.NewClient(context.Background(), env.Project)
		rtx.Must(err, "datastore client")
		saver = tracker.NewDatastoreSaver(dsiface.AdaptClient(client), dsKey)
	case "local":
		saver = tracker.NewLocalSaver(saverDir, dsKey)
	}

	tk, err := tracker.InitTracker(
		context.Background(), saver,
		time.Minute, *jobExpirationTime, *jobCleanupDelay)
	rtx.Must(err, "tracker init")
	if tk == nil {
		log.Fatal("nil tracker")
	}

	return tk
}

func mustCreateJobService(ctx context.Context) *job.Service {
	var saver persistence.Saver
	storageClient, err := storage.NewClient(ctx)
	rtx.Must(err, "Could not create storage client for job service")

	switch saverType.Value {
	case "datastore":
		saver, err = persistence.NewDatastoreSaver(context.Background(), project)
		rtx.Must(err, "Could not initialize datastore saver")
	case "local":
		saver = persistence.NewLocalSaver(saverDir)
	}
	svc, err := job.NewJobService(
		ctx, globalTracker, config.StartDate(),
		project, config.Sources(), saver,
		stiface.AdaptClient(storageClient))
	rtx.Must(err, "Could not initialize job service")
	return svc
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
		Addr:    *gardenerAddr,
		Handler: mux,
	}

	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)

	// TODO - do we want different health checks for manager mode?
	mux.HandleFunc("/alive", healthCheck)
	mux.HandleFunc("/ready", healthCheck)

	// This is the v2 "manager" mode, in which Gardener provides the "jobs" API
	// for parsers to request work and report progress.
	// TODO Once the legacy deployments are turned down, this should move to head of main().
	config.ParseConfig()

	for _, src := range config.Sources() {
		metrics.ConfigDatatypes.WithLabelValues(src.Experiment, src.Datatype)
	}

	globalTracker = mustStandardTracker()

	// TODO - refactor this block.
	cloudCfg := cloud.Config{
		Project: env.Project,
		Client:  nil,
	}
	bqConfig := NewBQConfig(cloudCfg)
	monitor, err := ops.NewStandardMonitor(mainCtx, project, bqConfig, globalTracker)
	rtx.Must(err, "NewStandardMonitor failed")
	go monitor.Watch(mainCtx, 5*time.Second)

	js := mustCreateJobService(mainCtx)
	handler := tracker.NewHandler(globalTracker, js)
	handler.Register(mux)

	isReady = true
	log.Println("Running as manager service")

	rtx.Must(httpx.ListenAndServeAsync(server), "Could not start main server")

	// Wait and shutdown.
	<-mainCtx.Done()
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
