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

func TestAudit1_Channel_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns channel with ARN and ingest endpoints",
			body:     map[string]any{"id": "my-channel", "description": "live stream"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:mediapackage:us-east-1:000000000000:channels/my-channel")
				assert.Equal(t, "my-channel", resp["id"])
				assert.Equal(t, "live stream", resp["description"])

				hlsIngest := resp["hlsIngest"].(map[string]any)
				ingestEndpoints := hlsIngest["ingestEndpoints"].([]any)
				assert.Len(t, ingestEndpoints, 2)
				ep0 := ingestEndpoints[0].(map[string]any)
				assert.NotEmpty(t, ep0["id"])
				assert.NotEmpty(t, ep0["url"])
				assert.NotEmpty(t, ep0["username"])
				assert.NotEmpty(t, ep0["password"])
			},
		},
		{
			name:     "create missing Id returns 422",
			body:     map[string]any{"description": "no id"},
			wantCode: http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channels", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_Channel_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	assert.Equal(t, 1, mediapackage.ChannelCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, channelID, descResp["id"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/channels/"+channelID, map[string]any{
		"description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated description", updateResp["description"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["channels"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/channels/"+channelID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 0, mediapackage.ChannelCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_Channel_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": "test-channel"})
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

func TestAudit1_Channel_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/channels/notexist"},
		{"update unknown returns 404", http.MethodPut, "/channels/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/channels/notexist"},
		{"rotate creds unknown returns 404", http.MethodPut, "/channels/notexist/credentials"},
		{"configure logs unknown returns 404", http.MethodPut, "/channels/notexist/configure_logs"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAudit1_Channel_RotateCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Get original credentials
	rec := doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	var before map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &before))
	hls := before["hlsIngest"].(map[string]any)
	eps := hls["ingestEndpoints"].([]any)
	originalPassword := eps[0].(map[string]any)["password"].(string)

	// Rotate
	rec = doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/credentials", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
	hls = after["hlsIngest"].(map[string]any)
	eps = hls["ingestEndpoints"].([]any)
	newPassword := eps[0].(map[string]any)["password"].(string)

	assert.NotEqual(t, originalPassword, newPassword, "credentials should rotate")
}

func TestAudit1_Channel_ConfigureLogs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/configure_logs", map[string]any{
		"egressAccessLogs":  map[string]any{"logGroupName": "/aws/MediaPackage/EgressAccessLogs"},
		"ingressAccessLogs": map[string]any{"logGroupName": "/aws/MediaPackage/IngressAccessLogs"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, channelID, resp["id"])
}

func TestAudit1_Channel_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["channels"])
}

func TestAudit1_OriginEndpoint_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns endpoint with ARN and URL",
			body: map[string]any{
				"channelId":    "ch1",
				"id":           "ep1",
				"description":  "HLS endpoint",
				"manifestName": "index",
				"origination":  "ALLOW",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:mediapackage:us-east-1:000000000000:origin_endpoints/ep1")
				assert.Equal(t, "ep1", resp["id"])
				assert.Equal(t, "ch1", resp["channelId"])
				assert.Equal(t, "ALLOW", resp["origination"])
				assert.NotEmpty(t, resp["url"])
			},
		},
		{
			name:     "create missing ChannelId returns 422",
			body:     map[string]any{"id": "ep1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create missing Id returns 422",
			body:     map[string]any{"channelId": "ch1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create channel not found returns 404",
			body:     map[string]any{"channelId": "nonexistent", "id": "ep1"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.wantCode == http.StatusCreated {
				// Pre-create the channel
				doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": "ch1"})
			}
			rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_OriginEndpoint_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)
	epID := createTestOriginEndpoint(t, h, channelID)

	assert.Equal(t, 1, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, epID, descResp["id"])
	assert.Equal(t, channelID, descResp["channelId"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/origin_endpoints/"+epID, map[string]any{
		"description": "updated endpoint",
		"origination": "DENY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated endpoint", updateResp["description"])
	assert.Equal(t, "DENY", updateResp["origination"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["originEndpoints"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 0, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_OriginEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/origin_endpoints/notexist"},
		{"update unknown returns 404", http.MethodPut, "/origin_endpoints/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/origin_endpoints/notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAudit1_OriginEndpoint_DefaultOrigination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": "test-channel",
		"id":        "ep-defaults",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ALLOW", resp["origination"])
	assert.Equal(t, "ep-defaults", resp["manifestName"])
}

func TestAudit1_OriginEndpoint_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["originEndpoints"])
}

func TestAudit1_DeleteChannel_CascadesEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)
	createTestOriginEndpoint(t, h, channelID)

	assert.Equal(t, 1, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))

	rec := doRequest(t, h, http.MethodDelete, "/channels/"+channelID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	assert.Equal(t, 0, mediapackage.ChannelCount(h.Backend.(*mediapackage.InMemoryBackend)))
	assert.Equal(t, 0, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))
}

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Get ARN
	rec := doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	resourceARN := descResp["arn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{
		"tags": map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// UntagResource
	req := httptest.NewRequest(http.MethodDelete, "/tags/"+resourceARN+"?tagKeys=env", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}
