package mediapackage_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func newTestHandler(t *testing.T) *mediapackage.Handler {
	t.Helper()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")

	return mediapackage.NewHandler(backend)
}

func doRequest(t *testing.T, h *mediapackage.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRequestJSON sends a request and returns the status code and the
// JSON-decoded response body (nil if the body was empty).
func doRequestJSON(t *testing.T, h *mediapackage.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()

	rec := doRequest(t, h, method, path, body)

	if rec.Body.Len() == 0 {
		return rec.Code, nil
	}

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

func createTestChannel(t *testing.T, h *mediapackage.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
		"id":          "test-channel",
		"description": "Test Channel",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

func createTestOriginEndpoint(t *testing.T, h *mediapackage.Handler, channelID string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": channelID,
		"id":        "test-endpoint",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// createChannelWithTags creates a channel with optional tags and returns its
// ID and ARN.
func createChannelWithTags(t *testing.T, h *mediapackage.Handler, id string, tags map[string]any) (string, string) {
	t.Helper()

	body := map[string]any{"id": id}
	if tags != nil {
		body["tags"] = tags
	}

	code, resp := doRequestJSON(t, h, http.MethodPost, "/channels", body)
	require.Equal(t, http.StatusCreated, code)

	return resp["id"].(string), resp["arn"].(string)
}

// createEndpointWithTags creates an origin endpoint with optional tags and
// returns its ARN.
func createEndpointWithTags(
	t *testing.T,
	h *mediapackage.Handler,
	channelID, epID string,
	tags map[string]any,
) string {
	t.Helper()

	body := map[string]any{"channelId": channelID, "id": epID}
	if tags != nil {
		body["tags"] = tags
	}

	code, resp := doRequestJSON(t, h, http.MethodPost, "/origin_endpoints", body)
	require.Equal(t, http.StatusCreated, code)

	return resp["arn"].(string)
}

// TestHandler_RouteMatcher_ChannelsServiceGating verifies that the shared
// "/channels" REST path (also used by IoT Analytics and MediaTailor) is only
// claimed by MediaPackage for SigV4-signed mediapackage requests, while the
// MediaPackage-exclusive paths match regardless of signing service.
func TestHandler_RouteMatcher_ChannelsServiceGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{name: "channels with mediapackage service", path: "/channels", service: "mediapackage", want: true},
		{name: "channel sub with mediapackage service", path: "/channels/c1", service: "mediapackage", want: true},
		{name: "channels with iotanalytics service", path: "/channels", service: "iotanalytics", want: false},
		{name: "channels with mediatailor service", path: "/channels", service: "mediatailor", want: false},
		{name: "channels without service", path: "/channels", want: false},
		{name: "origin endpoints without service", path: "/origin_endpoints", want: true},
		{name: "harvest jobs without service", path: "/harvest_jobs", want: true},
		{
			name: "mediapackage tag path without service",
			path: "/tags/arn:aws:mediapackage:us-east-1:000000000000:channels/c1",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.service != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/"+tt.service+"/aws4_request",
				)
			}

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// TestNotFound_ErrorType verifies that not-found responses include the
// __type field used by the AWS SDK for error classification, across every
// resource family's Describe operation.
func TestNotFound_ErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "describe missing channel includes __type", method: http.MethodGet, path: "/channels/no-such"},
		{name: "describe missing endpoint includes __type", method: http.MethodGet, path: "/origin_endpoints/no-such"},
		{name: "describe missing harvest job includes __type", method: http.MethodGet, path: "/harvest_jobs/no-such"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			code, resp := doRequestJSON(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, code)
			require.NotNil(t, resp)
			assert.Contains(t, resp, "__type", "__type field required for SDK error classification")
			assert.Contains(t, resp["__type"], "NotFoundException")
		})
	}
}
