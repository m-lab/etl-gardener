package tracker

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// URL is a string representing an url.
type URL = string

// Server is a string representing a server, e.g. http://10.100.1.2:8080
type Server = string

// UpdateURL makes an update request URL.
func UpdateURL(server Server, job Job, state State, detail string) (URL, error) {
	return fmt.Sprintf("%s/update?job=%s&state=%s&detail=%s",
		server, string(job.Marshal()), state, detail), nil
}

// Handler provides handlers for update, heartbeat, etc.
type Handler struct {
	tracker *Tracker
}

// NewHandler returns a Handler that sends updates to provided Tracker.
func NewHandler(tr *Tracker) *Handler {
	return &Handler{tr}
}

func getJob(jobString string) (Job, error) {
	var job Job
	if jobString == "" {
		return job, errors.New("Empty job")
	}
	err := json.Unmarshal([]byte(jobString), &job)
	return job, err
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
	job, err := getJob(req.Form.Get("job"))
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if err := h.tracker.Heartbeat(job); err != nil {
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
	job, err := getJob(req.Form.Get("job"))
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	state := req.Form.Get("state")
	if state == "" {
		resp.WriteHeader(http.StatusFailedDependency)
		return
	}
	// detail := req.Form.Get("detail")

	if err := h.tracker.SetStatus(job, State(state)); err != nil {
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
	jobErr := req.Form.Get("Error")
	job, err := getJob(req.Form.Get("Job"))
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if jobErr == "" {
		resp.WriteHeader(http.StatusFailedDependency)
		return
	}
	if err := h.tracker.SetJobError(job, jobErr); err != nil {
		resp.WriteHeader(http.StatusGone)
		return
	}
	h.tracker.SetStatus(job, ParseError)
	resp.WriteHeader(http.StatusOK)
}

// Register registers the handlers on the server.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/heartbeat", h.heartbeat)
	mux.HandleFunc("/update", h.update)
	mux.HandleFunc("/error", h.errorFunc)
}
