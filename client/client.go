package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/go/logx"

	"github.com/m-lab/etl-gardener/tracker"
)

// ErrMoreJSON is returned
var ErrMoreJSON = errors.New("JSON body not completely consumed")

var decodeLogEvery = logx.NewLogEvery(nil, 30*time.Second)

// ClientErrorTotal measures the different type of RPC client errors.
var ClientErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gardener_client_error_total",
	Help: "The total number client annotation RPC errors.",
}, []string{"type"})

func post(ctx context.Context, url url.URL) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, "POST", url.String(), nil)
	if reqErr != nil {
		return nil, 0, reqErr
	}
	resp, postErr := http.DefaultClient.Do(req)
	if postErr != nil {
		return nil, 0, postErr // Documentation says we can ignore body.
	}

	// Guaranteed to have a non-nil response and body.
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body) // Documentation recommends reading body.
	return b, resp.StatusCode, err
}

// NextJob is used by clients to get a new job from Gardener.
func NextJob(ctx context.Context, base url.URL) (tracker.JobWithTarget, error) {
	jobURL := base
	jobURL.Path = "job"

	job := tracker.JobWithTarget{}

	b, status, err := post(ctx, jobURL)
	if err != nil {
		return job, err
	}
	if status != http.StatusOK {
		return job, errors.New(http.StatusText(status))
	}

	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()

	err = decoder.Decode(&job)
	if err != nil {
		// TODO add metric, but in the correct namespace???
		// When this happens, it is likely to be very spammy.
		decodeLogEvery.Println("Decode error:", err)

		// Try again but ignore unknown fields.
		decoder = json.NewDecoder(bytes.NewReader(b))
		err = decoder.Decode(&job)
		if err != nil {
			// This is a more serious error.
			log.Println(err)
			ClientErrorTotal.WithLabelValues("json decode error").Inc()
			return job, err
		}
		if decoder.More() {
			decodeLogEvery.Println("Decode error:", ErrMoreJSON)
		}
	}
	return job, err
}
