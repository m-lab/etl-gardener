package gardener

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl-gardener/tracker/jobtest"
	"github.com/m-lab/go/testingx"
)

func TestJobClient_Next(t *testing.T) {
	start := time.Date(2019, time.June, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name             string
		server           *httptest.Server
		want             *tracker.JobWithTarget
		wantErr          bool
		corruptURL       bool
		corruptTransport bool
	}{
		{
			name: "success-with-id",
			server: httptest.NewUnstartedServer(
				// Returns a true tracker.JobWithTarget record.
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					j := jobtest.NewJob("bucket", "experiment", "datatype", start)
					job := tracker.JobWithTarget{
						ID:  j.Key(),
						Job: j,
					}
					b, _ := json.Marshal(&job)
					_, err := resp.Write(b)
					if err != nil {
						t.Fatal(err)
					}
				}),
			),
			want: &tracker.JobWithTarget{
				ID: "bucket/experiment/datatype/20190601",
				Job: tracker.Job{
					Bucket:     "bucket",
					Experiment: "experiment",
					Datatype:   "datatype",
					Date:       start,
					Datasets: tracker.Datasets{
						Tmp: "tmp_experiment",
						Raw: "raw_experiment",
					},
				},
			},
		},
		{
			name: "error-corrupt-reply",
			server: httptest.NewUnstartedServer(
				// Return a corrupted reply that cannot be Unmarshal'd.
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					_, err := resp.Write([]byte("-this-is-not-json-"))
					if err != nil {
						t.Fatal(err)
					}
				}),
			),
			wantErr: true,
		},
		{
			name: "error-server-status-error",
			server: httptest.NewUnstartedServer(
				// Return a non-200 HTTP status response.
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusInternalServerError)
				}),
			),
			wantErr: true,
		},
		{
			name: "error-corrupt-URL",
			server: httptest.NewUnstartedServer(
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusOK)
				}),
			),
			wantErr:    true,
			corruptURL: true,
		},
		{
			name: "error-corrupt-http-transport",
			server: httptest.NewUnstartedServer(
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusOK)
				}),
			),
			wantErr:          true,
			corruptTransport: true,
		},
	}
	for _, tt := range tests {
		tt.server.Start()
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.server.URL)
			testingx.Must(t, err, "failed to parse httptest server url: %q", tt.server.URL)
			if tt.corruptURL {
				// Corrupt the URL scheme so that creating a new HTTP request fails.
				u.Scheme = "-not-a-scheme-"
			}
			c := NewJobClient(*u)
			if tt.corruptTransport {
				// Corrupt the default http transport so that making the request fails.
				// Backup, reset for test, and restore.
				orig := http.DefaultTransport
				http.DefaultTransport = nil
				defer func() {
					http.DefaultTransport = orig
				}()
			}

			// Call Next job API.
			got, err := c.Next(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("JobClient.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JobClient.Next() = %#v, want %#v", got, tt.want)
			}
		})
		tt.server.CloseClientConnections()
		tt.server.Close()
	}
}

func TestJobClient_Update_Heartbeat_Error(t *testing.T) {
	tests := []struct {
		name    string
		server  *httptest.Server
		wantErr bool
	}{
		{
			name: "success",
			server: httptest.NewUnstartedServer(
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusOK)
					err := req.ParseForm()
					testingx.Must(t, err, "failed to parse form")
					id := req.Form.Get("id")
					if id == "" {
						t.Error("received request without ID")
					}
				}),
			),
		},
		{
			name: "error-server-status-error",
			server: httptest.NewUnstartedServer(
				// Return a non-200 HTTP status response.
				http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
					resp.WriteHeader(http.StatusInternalServerError)
				}),
			),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt.server.Start()
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.server.URL)
			testingx.Must(t, err, "failed to parse server url: %q", tt.server.URL)
			c := NewJobClient(*u)
			if err := c.Update(context.Background(), "id", tracker.Complete, "okay"); (err != nil) != tt.wantErr {
				t.Errorf("JobClient.Heartbeat() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := c.Heartbeat(context.Background(), "id"); (err != nil) != tt.wantErr {
				t.Errorf("JobClient.Heartbeat() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := c.Error(context.Background(), "id", "error message"); (err != nil) != tt.wantErr {
				t.Errorf("JobClient.Heartbeat() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		tt.server.CloseClientConnections()
		tt.server.Close()
	}
}
