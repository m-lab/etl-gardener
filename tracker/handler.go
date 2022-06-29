package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/m-lab/go/logx"
)

var (
	MsgNoJobFound = "No job found. Try again."
	MsgJobExists  = "Job already exists. Try again."
)

// UpdateURL makes an update request URL.
// TODO(soltesz): move client functions to client package.
func UpdateURL(base url.URL, job Job, state State, detail string) *url.URL {
	base.Path += "update"
	params := make(url.Values, 3)
	params.Add("job", string(job.Marshal()))
	params.Add("state", string(state))
	params.Add("detail", detail)

	base.RawQuery = params.Encode()
	return &base
}

// HeartbeatURL makes an update request URL.
// TODO(soltesz): move client functions to client package.
func HeartbeatURL(base url.URL, job Job) *url.URL {
	base.Path += "heartbeat"
	params := make(url.Values, 3)
	params.Add("job", string(job.Marshal()))

	base.RawQuery = params.Encode()
	return &base
}

// ErrorURL makes an update request URL.
// TODO(soltesz): move client functions to client package.
func ErrorURL(base url.URL, job Job, errString string) *url.URL {
	base.Path += "error"
	params := make(url.Values, 3)
	params.Add("job", string(job.Marshal()))
	params.Add("error", errString)

	base.RawQuery = params.Encode()
	return &base
}

type JobService interface {
	NextJob(ctx context.Context) JobWithTarget
}

// Handler provides handlers for update, heartbeat, etc.
type Handler struct {
	tracker    *Tracker
	jobservice JobService
}

// NewHandler returns a Handler that sends updates to provided Tracker.
func NewHandler(tr *Tracker, js JobService) *Handler {
	return &Handler{tracker: tr, jobservice: js}
}

func getJob(jobString string) (Job, error) {
	var job Job
	if jobString == "" {
		return job, errors.New("Empty job")
	}
	err := json.Unmarshal([]byte(jobString), &job)
	return job, err
}

func getID(req *http.Request) (Key, error) {
	// Prefer the /v2 "id" parameter.
	id := req.Form.Get("id")
	if id != "" {
		return Key(id), nil
	}

	// Fallback to the "job" parameter used by the original /job api.
	j := req.Form.Get("job")
	if j == "" {
		return "", errors.New("no job id found")
	}
	job, err := getJob(j)
	if err != nil {
		return "", err
	}
	return job.Key(), nil
}

func (h *Handler) heartbeat(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := req.ParseForm(); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	id, err := getID(req)
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if err := h.tracker.Heartbeat(id); err != nil {
		logx.Debug.Printf("%v %+v\n", err, id)
		resp.WriteHeader(http.StatusGone)
		return
	}
	resp.WriteHeader(http.StatusOK)
}

func (h *Handler) update(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := req.ParseForm(); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	id, err := getID(req)
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	state := req.Form.Get("state")
	if state == "" {
		resp.WriteHeader(http.StatusFailedDependency)
		return
	}
	detail := req.Form.Get("detail")

	if err := h.tracker.SetStatus(id, State(state), detail); err != nil {
		log.Printf("Not found %+v\n", id)
		resp.WriteHeader(http.StatusGone)
		return
	}
	resp.WriteHeader(http.StatusOK)
}

func (h *Handler) errorFunc(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := req.ParseForm(); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	jobErr := req.Form.Get("error")
	id, err := getID(req)
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if jobErr == "" {
		resp.WriteHeader(http.StatusFailedDependency)
		return
	}
	if err := h.tracker.SetStatus(id, ParseError, jobErr); err != nil {
		resp.WriteHeader(http.StatusGone)
		return
	}
	resp.WriteHeader(http.StatusOK)
}

func (h *Handler) nextJob(resp http.ResponseWriter, req *http.Request) *JobWithTarget {
	// Must be a post because it changes state.
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return nil
	}
	job := h.jobservice.NextJob(req.Context())

	// Check for empty job (no job found with files)
	if job.Job.Date.Equal(time.Time{}) {
		log.Println(MsgNoJobFound)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgNoJobFound))
		return nil
	}

	err := h.tracker.AddJob(job.Job)
	if err != nil {
		log.Println(err, job)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgJobExists))
		return nil
	}
	return &job
}

// nextJobV1 returns the next JobWithTarget and returns the tracker.Job to the requesting client.
func (h *Handler) nextJobV1(resp http.ResponseWriter, req *http.Request) {
	job := h.nextJob(resp, req)
	if job == nil {
		return
	}
	log.Printf("Dispatching %s\n", job.Job)
	_, err := resp.Write(job.Job.Marshal())
	if err != nil {
		log.Println(err)
		return
	}
}

// nextJobV2 returns the next JobWithTarget to the requesting client.
func (h *Handler) nextJobV2(resp http.ResponseWriter, req *http.Request) {
	job := h.nextJob(resp, req)
	if job == nil {
		return
	}
	log.Printf("Dispatching %s\n", job.Job)
	// Marshal complete JobWithTarget object.
	b, _ := json.Marshal(job)
	_, err := resp.Write(b)
	if err != nil {
		log.Println(err)
		return
	}
}

// Register registers the handlers on the server.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/heartbeat", h.heartbeat)
	mux.HandleFunc("/update", h.update)
	mux.HandleFunc("/error", h.errorFunc)
	mux.HandleFunc("/job", h.nextJobV1)

	mux.HandleFunc("/v2/job/heartbeat", h.heartbeat)
	mux.HandleFunc("/v2/job/update", h.update)
	mux.HandleFunc("/v2/job/error", h.errorFunc)
	mux.HandleFunc("/v2/job/next", h.nextJobV2)
}
