package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestChannel_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create returns channel with ARN and STOPPED state",
			path: "/channel/my-channel",
			body: map[string]any{
				"PlaybackMode": "LOOP",
				"Outputs": []any{
					map[string]any{"ManifestName": "index", "SourceGroup": "hd"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-channel", resp["ChannelName"])
				assert.Contains(t, resp["Arn"], "arn:aws:mediatailor:us-east-1:000000000000:channel/my-channel")
				assert.Equal(t, "STOPPED", resp["ChannelState"])
				assert.Equal(t, "LOOP", resp["PlaybackMode"])
				assert.Len(t, resp["Outputs"], 1)
			},
		},
		{
			name:     "create missing ChannelName in path still works",
			path:     "/channel/ch2",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
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

	// Create
	rec := doRequest(t, h, http.MethodPost, "/channel/test-channel", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.ChannelCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-channel", descResp["ChannelName"])
	assert.Equal(t, "STOPPED", descResp["ChannelState"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/channel/test-channel", map[string]any{
		"Outputs": []any{
			map[string]any{"ManifestName": "index", "SourceGroup": "hd"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Len(t, updateResp["Outputs"], 1)

	// List
	rec = doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, mediatailor.ChannelCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestChannel_StartStop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})

	// Start
	rec := doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify running
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1", nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "RUNNING", descResp["ChannelState"])

	// Start again is idempotent
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stopped
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPED", descResp["ChannelState"])

	// Stop again is idempotent
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestChannel_DeleteRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)

	rec := doRequest(t, h, http.MethodDelete, "/channel/ch1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestChannel_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
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
		{"describe unknown returns 404", http.MethodGet, "/channel/notexist"},
		{"update unknown returns 404", http.MethodPut, "/channel/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/channel/notexist"},
		{"start unknown returns 404", http.MethodPut, "/channel/notexist/start"},
		{"stop unknown returns 404", http.MethodPut, "/channel/notexist/stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestChannel_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

func TestListChannels_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 2 {
		name := "ch-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPost, "/channel/"+name, map[string]any{
			"PlaybackMode": "LINEAR",
			"Outputs": []any{
				map[string]any{
					"ManifestName": "manifest",
					"SourceGroup":  "default",
				},
			},
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}

func TestChannelOutput_WithHLS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LINEAR",
		"Outputs": []any{
			map[string]any{
				"ManifestName": "index",
				"SourceGroup":  "main",
				"HlsPlaylistSettings": map[string]any{
					"ManifestWindowSeconds": float64(30),
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	outputs, _ := resp["Outputs"].([]any)
	require.Len(t, outputs, 1)
	out, _ := outputs[0].(map[string]any)
	assert.NotNil(t, out["HlsPlaylistSettings"])
}

func TestCreateChannel_MissingPlaybackMode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code, "create channel without playback mode defaults to LOOP")
}

// TestCreateChannel_PlaybackModeValidation verifies CreateChannel validates
// PlaybackMode.
func TestCreateChannel_PlaybackModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		playbackMode string
		wantStatus   int
	}{
		{name: "LOOP_accepted", playbackMode: "LOOP", wantStatus: http.StatusOK},
		{name: "LINEAR_accepted", playbackMode: "LINEAR", wantStatus: http.StatusOK},
		{name: "empty_defaults_to_LOOP", playbackMode: "", wantStatus: http.StatusOK},
		{name: "INVALID_rejected", playbackMode: "INVALID", wantStatus: http.StatusBadRequest},
		{name: "loop_lowercase_rejected", playbackMode: "loop", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
				"PlaybackMode": tt.playbackMode,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestCreateChannel_ReturnsTier verifies CreateChannel returns the Tier field.
func TestCreateChannel_ReturnsTier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channel/tier-ch", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BASIC", resp["Tier"], "Tier must default to BASIC")
}

// TestChannel_StartStopIdempotent verifies StartChannel and StopChannel are
// idempotent.
func TestChannel_StartStopIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})

	// Start: STOPPED → RUNNING.
	rec := doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "start stopped channel must succeed")

	// Start again: RUNNING → RUNNING (idempotent).
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "start already-running channel must be idempotent")

	// Stop: RUNNING → STOPPED.
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "stop running channel must succeed")

	// Stop again: STOPPED → STOPPED (idempotent).
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "stop already-stopped channel must be idempotent")
}

// TestCreateChannel_FillerSlate verifies CreateChannel accepts and returns
// FillerSlate.
func TestCreateChannel_FillerSlate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
	})
	doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
		},
	})

	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LOOP",
		"FillerSlate": map[string]any{
			"SourceLocationName": "sl1",
			"VodSourceName":      "vs1",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	fs, ok := resp["FillerSlate"].(map[string]any)
	require.True(t, ok, "FillerSlate must be present in response")
	assert.Equal(t, "sl1", fs["SourceLocationName"])
	assert.Equal(t, "vs1", fs["VodSourceName"])
}

// TestCreateChannel_TagsSurviveDescribe verifies tags passed to CreateChannel
// are queryable back from DescribeChannel/ListChannels. Regression test for a
// bug found by field-diffing this pass: CreateChannel stored tags on the
// channel struct but DescribeChannel/ListChannels unconditionally overwrite
// the response Tags from a separate ARN-keyed tag map that CreateChannel
// never wrote to, silently dropping every tag passed at creation.
func TestCreateChannel_TagsSurviveDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LOOP",
		"tags":         map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	createdTags, _ := created["tags"].(map[string]any)
	assert.Equal(t, "prod", createdTags["env"], "tags must be present in the create response")

	rec = doRequest(t, h, http.MethodGet, "/channel/ch1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &described))
	describedTags, _ := described["tags"].(map[string]any)
	assert.Equal(t, "prod", describedTags["env"], "tags set at creation must survive to DescribeChannel")
}

// TestChannel_AudiencesAndTimeShift verifies CreateChannel/UpdateChannel
// accept and return Audiences and TimeShiftConfiguration (deferred item:
// these fields weren't modeled at all previously).
func TestChannel_AudiencesAndTimeShift(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LOOP",
		"Audiences":    []any{"aud1", "aud2"},
		"TimeShiftConfiguration": map[string]any{
			"MaxTimeDelaySeconds": float64(120),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	audiences, _ := resp["Audiences"].([]any)
	assert.ElementsMatch(t, []any{"aud1", "aud2"}, audiences)
	ts, ok := resp["TimeShiftConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(120), ts["MaxTimeDelaySeconds"], 0.0001)

	rec = doRequest(t, h, http.MethodPut, "/channel/ch1", map[string]any{
		"Audiences": []any{"aud3"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	updateResp := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	audiences, _ = updateResp["Audiences"].([]any)
	assert.ElementsMatch(t, []any{"aud3"}, audiences)
	assert.Nil(t, updateResp["TimeShiftConfiguration"], "UpdateChannel omitting TimeShiftConfiguration must clear it")
}

// TestCreateChannel_Tier verifies CreateChannel accepts an explicit Tier and
// rejects an invalid one.
func TestCreateChannel_Tier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tier       string
		wantTier   string
		wantStatus int
	}{
		{name: "STANDARD accepted", tier: "STANDARD", wantStatus: http.StatusOK, wantTier: "STANDARD"},
		{name: "BASIC accepted", tier: "BASIC", wantStatus: http.StatusOK, wantTier: "BASIC"},
		{name: "empty defaults to BASIC", tier: "", wantStatus: http.StatusOK, wantTier: "BASIC"},
		{name: "invalid rejected", tier: "GOLD", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
				"PlaybackMode": "LOOP",
				"Tier":         tt.tier,
			})
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantTier, resp["Tier"])
			}
		})
	}
}

// TestDeleteChannel_CascadesPrograms verifies DeleteChannel removes every
// program scheduled on the channel and its policy, so no ghost rows survive
// in the programs table (or its byChannel index) after the channel is gone.
func TestDeleteChannel_CascadesPrograms(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{"PlaybackMode": "LOOP"})
	doRequest(t, h, http.MethodPut, "/channel/ch1/policy", map[string]any{
		"Policy": `{"Version":"2012-10-17"}`,
	})

	progBody := testScheduleConfigBody(1_700_000_000_000)
	progBody["SourceLocationName"] = "sl1"
	progRec := doRequest(t, h, http.MethodPost, "/channel/ch1/program/prog1", progBody)
	require.Equal(t, http.StatusOK, progRec.Code, progRec.Body.String())

	rec := doRequest(t, h, http.MethodDelete, "/channel/ch1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Recreating the channel must not resurrect the old program or policy --
	// both must have been cascade-deleted, not merely orphaned.
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{"PlaybackMode": "LOOP"})

	rec = doRequest(t, h, http.MethodGet, "/channel/ch1/program/prog1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code, "program must not survive channel delete+recreate")

	rec = doRequest(t, h, http.MethodGet, "/channel/ch1/policy", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code, "policy must not survive channel delete+recreate")
}
