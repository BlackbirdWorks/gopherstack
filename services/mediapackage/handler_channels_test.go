package mediapackage_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func TestChannel_Create(t *testing.T) {
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

func TestChannel_CRUD(t *testing.T) {
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

func TestChannel_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": "test-channel"})
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

func TestChannel_NotFound(t *testing.T) {
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

func TestChannel_RotateCredentials(t *testing.T) {
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

func TestChannel_ConfigureLogs(t *testing.T) {
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

func TestChannel_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["channels"])
}

func TestChannel_DeleteCascadesEndpoints(t *testing.T) {
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

// TestChannel_ARNShape verifies ARN format matches AWS pattern.
func TestChannel_ARNShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		channelID  string
		wantPrefix string
	}{
		{
			name:       "channel ARN has correct prefix and resource",
			channelID:  "my-live-ch",
			wantPrefix: "arn:aws:mediapackage:us-east-1:000000000000:channels/my-live-ch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, arn := createChannelWithTags(t, h, tc.channelID, nil)
			assert.Equal(t, tc.wantPrefix, arn)
		})
	}
}

// TestChannel_DeleteReturns202 verifies the delete channel returns 202 Accepted.
func TestChannel_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete channel returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createChannelWithTags(t, h, "ch-del-202", nil)

			code, _ := doRequestJSON(t, h, http.MethodDelete, "/channels/ch-del-202", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestHandler_RotateChannelCredentials_RealPath verifies RotateChannelCredentials
// is routed at PUT /channels/{id}/credentials, matching the real
// aws-sdk-go-v2/service/mediapackage wire shape. The route was previously
// wired to POST /channels/{id}/ingest_endpoints/credentials, a path the real
// SDK never sends, so a genuine client call would have 404'd as "unknown
// operation" -- exactly the disguised-route-matcher-bug class unit tests
// (calling h.Handler() with a made-up path) can hide.
func TestHandler_RotateChannelCredentials_RealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/credentials", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, channelID, resp["id"])

	// The old (fictional) path must no longer be recognized as this operation.
	rec = doRequest(t, h, http.MethodPost, "/channels/"+channelID+"/ingest_endpoints/credentials", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_ConfigureLogs_ReflectsInDescribe verifies that log groups set via
// ConfigureLogs appear in a subsequent DescribeChannel response. Previously
// ConfigureLogs accepted egressAccessLogs/ingressAccessLogs, discarded them,
// and channelOutput never even had fields to carry them.
func TestHandler_ConfigureLogs_ReflectsInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/configure_logs", map[string]any{
		"egressAccessLogs":  map[string]any{"logGroupName": "/aws/MediaPackage/Egress"},
		"ingressAccessLogs": map[string]any{"logGroupName": "/aws/MediaPackage/Ingress"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	egress, ok := resp["egressAccessLogs"].(map[string]any)
	require.True(t, ok, "egressAccessLogs should be present after ConfigureLogs")
	assert.Equal(t, "/aws/MediaPackage/Egress", egress["logGroupName"])

	ingress, ok := resp["ingressAccessLogs"].(map[string]any)
	require.True(t, ok, "ingressAccessLogs should be present after ConfigureLogs")
	assert.Equal(t, "/aws/MediaPackage/Ingress", ingress["logGroupName"])
}

// TestHandler_Channel_CreatedAtPresent verifies the createdAt field --
// present on every real MediaPackage Channel response -- is populated
// rather than absent from the JSON entirely.
func TestHandler_Channel_CreatedAtPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["createdAt"])
}

func TestRotateIngestEndpointCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte, oldPassword string)
		name     string
		wantCode int
		badEpID  bool
		badChID  bool
	}{
		{
			name:     "rotate credentials changes password",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, oldPassword string) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])

				hls := resp["hlsIngest"].(map[string]any)
				eps := hls["ingestEndpoints"].([]any)
				require.NotEmpty(t, eps)

				// Find the rotated endpoint — at least one should have changed password
				rotated := false
				for _, ep := range eps {
					epm := ep.(map[string]any)
					if epm["password"].(string) != oldPassword {
						rotated = true

						break
					}
				}

				assert.True(t, rotated, "expected at least one endpoint to have new password")
			},
		},
		{
			name:     "bad channel returns not found",
			badChID:  true,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "bad ingest endpoint returns not found",
			badEpID:  true,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			h := mediapackage.NewHandler(backend)

			// Create channel and capture original ingest endpoint info
			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
				"id": "ch-rotate",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var chResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &chResp))
			hls := chResp["hlsIngest"].(map[string]any)
			eps := hls["ingestEndpoints"].([]any)
			require.NotEmpty(t, eps)

			firstEP := eps[0].(map[string]any)
			epID := firstEP["id"].(string)
			oldPassword := firstEP["password"].(string)

			channelID := "ch-rotate"
			ingestEPID := epID

			if tc.badChID {
				channelID = "no-such-channel"
			}

			if tc.badEpID {
				ingestEPID = "no-such-ep"
			}

			path := fmt.Sprintf("/channels/%s/ingest_endpoints/%s/credentials", channelID, ingestEPID)
			rec2 := doRequest(t, h, http.MethodPut, path, nil)
			assert.Equal(t, tc.wantCode, rec2.Code)

			if tc.check != nil {
				tc.check(t, rec2.Body.Bytes(), oldPassword)
			}
		})
	}
}
