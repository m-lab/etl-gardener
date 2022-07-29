package tracker

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/m-lab/go/logx"
)

var (
	MsgNoJobFound = "No job found. Try again."
	MsgJobExists  = "Job already exists. Try again."
)

type JobService interface {
	NextJob(ctx context.Context) *JobWithTarget
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

func (h *Handler) heartbeat(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := req.ParseForm(); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	id := Key(req.Form.Get("id"))
	if id == "" {
		resp.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	if err := h.tracker.Heartbeat(id); err != nil {
		logx.Debug.Printf("Heartbeat(%q) failed: %v\n", id, err)
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
	id := Key(req.Form.Get("id"))
	if id == "" {
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
	id := Key(req.Form.Get("id"))
	jobErr := req.Form.Get("error")
	if id == "" {
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
	jt := h.jobservice.NextJob(req.Context())

	// Check for empty job (no job found with files)
	if jt.Job.Date.Equal(time.Time{}) {
		log.Println(MsgNoJobFound)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgNoJobFound))
		return nil
	}

	err := h.tracker.AddJob(jt.Job)
	if err != nil {
		log.Println(err, jt)
		resp.WriteHeader(http.StatusInternalServerError)
		resp.Write([]byte(MsgJobExists))
		return nil
	}
	return jt
}

// nextJobV2 returns the next JobWithTarget to the requesting client.
func (h *Handler) nextJobV2(resp http.ResponseWriter, req *http.Request) {
	jt := h.nextJob(resp, req)
	if jt == nil {
		return
	}
	log.Printf("Dispatching %s\n", jt.Job)
	_, err := resp.Write(jt.Marshal())
	if err != nil {
		log.Println(err)
		return
	}
}

// Register registers the handlers on the server.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/v2/job/heartbeat", h.heartbeat)
	mux.HandleFunc("/v2/job/update", h.update)
	mux.HandleFunc("/v2/job/error", h.errorFunc)
	mux.HandleFunc("/v2/job/next", h.nextJobV2)
}
