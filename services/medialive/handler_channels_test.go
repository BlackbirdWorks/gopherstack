package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
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
			name:     "create returns channel with ARN and IDLE state",
			body:     map[string]any{"name": "my-channel", "channelClass": "STANDARD"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ch := resp["channel"].(map[string]any)
				assert.Contains(t, ch["arn"], "arn:aws:medialive:us-east-1:000000000000:channel:")
				assert.Equal(t, "IDLE", ch["state"])
				assert.Equal(t, "STANDARD", ch["channelClass"])
				assert.NotEmpty(t, ch["id"])
			},
		},
		{
			name:     "create missing Name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/channels", tc.body)
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

	assert.Equal(t, 1, medialive.ChannelCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-channel", descResp["name"])
	assert.Equal(t, "IDLE", descResp["state"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+channelID, map[string]any{
		"name": "updated-channel",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	ch := updateResp["channel"].(map[string]any)
	assert.Equal(t, "updated-channel", ch["name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["channels"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.ChannelCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestChannel_StartStop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Start
	rec := doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "STARTING", startResp["state"])

	// Start again returns conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "STOPPING", stopResp["state"])
}

// TestChannel_PipelinesRunningCount locks in the wire-accurate
// pipelinesRunningCount field (real DescribeChannelOutput reports the
// number of currently healthy pipelines: 0 while IDLE/STARTING/STOPPING, 2
// for a STANDARD channel once RUNNING).
func TestChannel_PipelinesRunningCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.InDelta(t, 0, descResp["pipelinesRunningCount"], 0, "IDLE channel reports 0 running pipelines")

	rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.InDelta(t, 0, startResp["pipelinesRunningCount"], 0, "STARTING channel reports 0 running pipelines")

	// The backend advances internal state to RUNNING immediately (see
	// StartChannel's doc comment), so an immediate Describe should already
	// report the class's full pipeline count.
	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "RUNNING", descResp["state"])
	assert.InDelta(t, 2, descResp["pipelinesRunningCount"], 0, "RUNNING STANDARD channel reports 2 running pipelines")
}

func TestChannel_DeleteRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Start channel
	doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)

	// Delete running channel returns conflict
	rec := doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestChannel_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/prod/channels/notexist"},
		{"update unknown returns 404", http.MethodPut, "/prod/channels/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/prod/channels/notexist"},
		{"start unknown returns 404", http.MethodPost, "/prod/channels/notexist/start"},
		{"stop unknown returns 404", http.MethodPost, "/prod/channels/notexist/stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestListChannels_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["channels"])
}

func TestChannelLifecycleExtras(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/prod/channels/"+channelID+"/channelClass",
		map[string]any{
			"channelClass": "SINGLE_PIPELINE",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	ch := decodeBody(t, rec.Body.Bytes())["channel"].(map[string]any)
	assert.Equal(t, "SINGLE_PIPELINE", ch["channelClass"])

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/channels/"+channelID+"/restartChannelPipelines",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/thumbnails", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["ThumbnailDetails"])

	rec = doRequest(t, h, http.MethodPut, "/prod/channels/missing/channelClass", map[string]any{
		"channelClass": "STANDARD",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAlertsAndVersions covers ListAlerts (per-channel) and ListVersions
// (account-wide channel engine versions), which share a handler section.
func TestAlertsAndVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/alerts", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["alerts"])

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/missing/alerts", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	versions := decodeBody(t, rec.Body.Bytes())["versions"].([]any)
	require.NotEmpty(t, versions)
	first := versions[0].(map[string]any)
	assert.NotEmpty(t, first["version"])
	_, hasExpiration := first["expirationDate"]
	assert.False(t, hasExpiration, "empty expirationDate must be omitted, not emitted as \"\"")
}
