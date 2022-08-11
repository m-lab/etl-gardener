package gardener

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
)

// JobClient manages requests to the Gardener Jobs API.
type JobClient struct {
	// Client is used for all HTTP requests to the Gardener API. A
	// NewJobClient uses http.DefaultClient but may be overridden.
	Client *http.Client
	api    url.URL
}

// NewJobClient creates a new JobClient that targets the given base API URL.
func NewJobClient(api url.URL) *JobClient {
	return &JobClient{api: api, Client: http.DefaultClient}
}

// Next requests a new parse job from the Gardener Jobs API.
func (c *JobClient) Next(ctx context.Context) (*tracker.JobWithTarget, error) {
	u := c.api
	u.Path = "/v2/job/next"
	b, err := c.postReadBody(ctx, u)
	if err != nil {
		return nil, err
	}
	job := &tracker.JobWithTarget{}
	err = json.Unmarshal(b, job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Update sets a new state with given detail for the given job ID.
func (c *JobClient) Update(ctx context.Context, id tracker.Key, state tracker.State, detail string) error {
	u := c.api
	u.Path = "/v2/job/update"
	params := make(url.Values, 3)
	params.Add("id", string(id))
	params.Add("state", string(state))
	params.Add("detail", detail)
	u.RawQuery = params.Encode()
	return c.postClose(ctx, u)
}

// Heartbeat sends a heartbeat message for the given job ID, notifying Gardener
// that the job is still in progress.
func (c *JobClient) Heartbeat(ctx context.Context, id tracker.Key) error {
	u := c.api
	u.Path = "/v2/job/heartbeat"
	params := make(url.Values, 3)
	params.Add("id", string(id))
	u.RawQuery = params.Encode()
	return c.postClose(ctx, u)
}

// Error reports an error message for the given job ID.
func (c *JobClient) Error(ctx context.Context, id tracker.Key, errString string) error {
	u := c.api
	u.Path = "/v2/job/error"
	params := make(url.Values, 3)
	params.Add("id", string(id))
	params.Add("error", errString)
	u.RawQuery = params.Encode()
	return c.postClose(ctx, u)
}

// postResp issues the request.
func (c *JobClient) postResp(ctx context.Context, u url.URL) (*http.Response, error) {
	// Limit posts to a constant 1 Minute timeout.
	ctxUpdate, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxUpdate, http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(http.StatusText(resp.StatusCode))
	}
	return resp, nil
}

// postReadBody issues post and reads response body.
func (c *JobClient) postReadBody(ctx context.Context, u url.URL) ([]byte, error) {
	resp, err := c.postResp(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// postClose posts the given URL and ignores the response.
func (c *JobClient) postClose(ctx context.Context, u url.URL) error {
	resp, err := c.postResp(ctx, u)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
