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
func HeartbeatURL(base url.URL, job Job) *url.URL {
	base.Path += "heartbeat"
	params := make(url.Values, 3)
	params.Add("job", string(job.Marshal()))

	base.RawQuery = params.Encode()
	return &base
}

// ErrorURL makes an update request URL.
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
		logx.Debug.Printf("%v %+v\n", err, job)
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
	detail := req.Form.Get("detail")

	if err := h.tracker.SetStatus(job, State(state), detail); err != nil {
		log.Printf("Not found %+v\n", job)
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
	job, err := getJob(req.Form.Get("job"))
	if err != nil {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if jobErr == "" {
		resp.WriteHeader(http.StatusFailedDependency)
		return
	}
	if err := h.tracker.SetStatus(job, ParseError, jobErr); err != nil {
		resp.WriteHeader(http.StatusGone)
		return
	}
	resp.WriteHeader(http.StatusOK)
}

func (h *Handler) nextJob(resp http.ResponseWriter, req *http.Request) {
	// Must be a post because it changes state.
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	job := h.jobservice.NextJob(req.Context())

	// Check for empty job (no job found with files)
	if job.Date.Equal(time.Time{}) {
		log.Println(MsgNoJobFound)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgNoJobFound))
		return
	}

	err := h.tracker.AddJob(job.Job)
	if err != nil {
		log.Println(err, job)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgJobExists))
		return
	}

	log.Printf("Dispatching %s\n", job.Job)
	_, err = resp.Write(job.Marshal())
	if err != nil {
		log.Println(err)
		// This should precede the Write(), but the Write failed, so this
		// is likely ok.
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Register registers the handlers on the server.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/heartbeat", h.heartbeat)
	mux.HandleFunc("/update", h.update)
	mux.HandleFunc("/error", h.errorFunc)
	mux.HandleFunc("/job", h.nextJob)
}
