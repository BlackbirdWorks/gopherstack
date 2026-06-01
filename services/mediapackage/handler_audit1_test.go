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

func createTestChannel(t *testing.T, h *mediapackage.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
		"Id":          "test-channel",
		"Description": "Test Channel",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Id"].(string)
}

func createTestOriginEndpoint(t *testing.T, h *mediapackage.Handler, channelID string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"ChannelId": channelID,
		"Id":        "test-endpoint",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Id"].(string)
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
			body:     map[string]any{"Id": "my-channel", "Description": "live stream"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["Arn"], "arn:aws:mediapackage:us-east-1:000000000000:channels/my-channel")
				assert.Equal(t, "my-channel", resp["Id"])
				assert.Equal(t, "live stream", resp["Description"])

				hlsIngest := resp["HlsIngest"].(map[string]any)
				ingestEndpoints := hlsIngest["IngestEndpoints"].([]any)
				assert.Len(t, ingestEndpoints, 2)
				ep0 := ingestEndpoints[0].(map[string]any)
				assert.NotEmpty(t, ep0["Id"])
				assert.NotEmpty(t, ep0["Url"])
				assert.NotEmpty(t, ep0["Username"])
				assert.NotEmpty(t, ep0["Password"])
			},
		},
		{
			name:     "create missing Id returns 422",
			body:     map[string]any{"Description": "no id"},
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
	assert.Equal(t, channelID, descResp["Id"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/channels/"+channelID, map[string]any{
		"Description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated description", updateResp["Description"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Channels"], 1)

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

	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"Id": "test-channel"})
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
		{"rotate creds unknown returns 404", http.MethodPost, "/channels/notexist/ingest_endpoints/credentials"},
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
	hls := before["HlsIngest"].(map[string]any)
	eps := hls["IngestEndpoints"].([]any)
	originalPassword := eps[0].(map[string]any)["Password"].(string)

	// Rotate
	rec = doRequest(t, h, http.MethodPost, "/channels/"+channelID+"/ingest_endpoints/credentials", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
	hls = after["HlsIngest"].(map[string]any)
	eps = hls["IngestEndpoints"].([]any)
	newPassword := eps[0].(map[string]any)["Password"].(string)

	assert.NotEqual(t, originalPassword, newPassword, "credentials should rotate")
}

func TestAudit1_Channel_ConfigureLogs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/configure_logs", map[string]any{
		"EgressAccessLogs":  map[string]any{"LogGroupName": "/aws/MediaPackage/EgressAccessLogs"},
		"IngressAccessLogs": map[string]any{"LogGroupName": "/aws/MediaPackage/IngressAccessLogs"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, channelID, resp["Id"])
}

func TestAudit1_Channel_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Channels"])
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
				"ChannelId":    "ch1",
				"Id":           "ep1",
				"Description":  "HLS endpoint",
				"ManifestName": "index",
				"Origination":  "ALLOW",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["Arn"], "arn:aws:mediapackage:us-east-1:000000000000:origin_endpoints/ep1")
				assert.Equal(t, "ep1", resp["Id"])
				assert.Equal(t, "ch1", resp["ChannelId"])
				assert.Equal(t, "ALLOW", resp["Origination"])
				assert.NotEmpty(t, resp["Url"])
			},
		},
		{
			name:     "create missing ChannelId returns 422",
			body:     map[string]any{"Id": "ep1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create missing Id returns 422",
			body:     map[string]any{"ChannelId": "ch1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create channel not found returns 404",
			body:     map[string]any{"ChannelId": "nonexistent", "Id": "ep1"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.wantCode == http.StatusCreated {
				// Pre-create the channel
				doRequest(t, h, http.MethodPost, "/channels", map[string]any{"Id": "ch1"})
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
	assert.Equal(t, epID, descResp["Id"])
	assert.Equal(t, channelID, descResp["ChannelId"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/origin_endpoints/"+epID, map[string]any{
		"Description": "updated endpoint",
		"Origination": "DENY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated endpoint", updateResp["Description"])
	assert.Equal(t, "DENY", updateResp["Origination"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["OriginEndpoints"], 1)

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
		"ChannelId": "test-channel",
		"Id":        "ep-defaults",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ALLOW", resp["Origination"])
	assert.Equal(t, "ep-defaults", resp["ManifestName"])
}

func TestAudit1_OriginEndpoint_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["OriginEndpoints"])
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
	resourceARN := descResp["Arn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{
		"Tags": map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
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
	tags = listResp["Tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}
