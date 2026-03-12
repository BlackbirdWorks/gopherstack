package iotanalytics_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// newTestHandler creates an in-memory backend + handler for HTTP tests.
func newTestHandler(t *testing.T) *iotanalytics.Handler {
	t.Helper()

	return iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
}

// doRequest performs an HTTP request against the handler.
func doRequest(t *testing.T, h *iotanalytics.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateAndDescribeChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		channel    string
		wantStatus int
	}{
		{
			name:       "success",
			channel:    "test-channel",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "empty_name",
			channel:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
				"channelName": tt.channel,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				rec2 := doRequest(t, h, http.MethodGet, "/channels/"+tt.channel, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_ListChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    []string
		wantLen int
	}{
		{
			name:    "empty",
			wantLen: 0,
		},
		{
			name:    "with_channels",
			seed:    []string{"ch1", "ch2"},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
					"channelName": name,
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/channels", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)

			summaries, ok := resp["channelSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

func TestHandler_DeleteChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		seed        bool
		wantStatus  int
	}{
		{
			name:        "success",
			channelName: "to-delete",
			seed:        true,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "not_found",
			channelName: "nonexistent",
			seed:        false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
					"channelName": tt.channelName,
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodDelete, "/channels/"+tt.channelName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAndDescribeDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		wantStatus    int
	}{
		{
			name:          "success",
			datastoreName: "test-datastore",
			wantStatus:    http.StatusCreated,
		},
		{
			name:          "empty_name",
			datastoreName: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datastores", map[string]string{
				"datastoreName": tt.datastoreName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				rec2 := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_CreateAndDescribeDataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		wantStatus  int
	}{
		{
			name:        "success",
			datasetName: "test-dataset",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "empty_name",
			datasetName: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{
				"datasetName": tt.datasetName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAndDescribePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		wantStatus   int
	}{
		{
			name:         "success",
			pipelineName: "test-pipeline",
			wantStatus:   http.StatusCreated,
		},
		{
			name:         "empty_name",
			pipelineName: "",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{
				"pipelineName": tt.pipelineName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				rec2 := doRequest(t, h, http.MethodGet, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
	matcher := h.RouteMatcher()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{
			name: "channels",
			path: "/channels",
			want: true,
		},
		{
			name: "channels_name",
			path: "/channels/my-channel",
			want: true,
		},
		{
			name: "datastores",
			path: "/datastores",
			want: true,
		},
		{
			name: "datasets",
			path: "/datasets",
			want: true,
		},
		{
			name: "pipelines",
			path: "/pipelines",
			want: true,
		},
		{
			name:    "tags_with_iotanalytics",
			path:    "/tags",
			service: "iotanalytics",
			want:    true,
		},
		{
			name:    "tags_without_service",
			path:    "/tags",
			service: "",
			want:    false,
		},
		{
			name: "other_path",
			path: "/vaults",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			if tt.service != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/"+tt.service+"/aws4_request",
				)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}
