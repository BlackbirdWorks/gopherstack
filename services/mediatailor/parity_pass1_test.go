package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// helperPutConfig creates a playback config with both required fields.
func helperPutConfig(t *testing.T, h *mediatailor.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  name,
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// ─────────────────────────────────────────────────────────────
// Gap 1: PutPlaybackConfiguration requires only Name.
//
// A previous pass had this backwards: it required AdDecisionServerUrl and
// VideoContentSourceUrl too. The real model's only required member is Name —
// confirmed against aws-sdk-go-v2/service/mediatailor's validators.go and
// botocore's service-2.json ("required": ["Name"]). Requiring the other two
// rejects requests a real SDK client is allowed to send.
// ─────────────────────────────────────────────────────────────

func TestParity_PutPlaybackConfiguration_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "all_fields_present_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                  "cfg",
				"AdDecisionServerUrl":   "https://ads.example.com",
				"VideoContentSourceUrl": "https://video.example.com",
			},
		},
		{
			name:       "missing_AdDecisionServerUrl_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                  "cfg",
				"VideoContentSourceUrl": "https://video.example.com",
			},
		},
		{
			name:       "missing_VideoContentSourceUrl_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                "cfg",
				"AdDecisionServerUrl": "https://ads.example.com",
			},
		},
		{
			name:       "name_only_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name": "cfg",
			},
		},
		{
			name:       "missing_Name_rejected",
			wantStatus: http.StatusBadRequest,
			body:       map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 2: PutPlaybackConfiguration replaces tags on update (not merges)
// ─────────────────────────────────────────────────────────────

func TestParity_PutPlaybackConfiguration_TagsReplacedOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
		"tags":                  map[string]any{"old": "val"},
	})

	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
		"tags":                  map[string]any{"new": "val"},
	})

	rec := doRequest(t, h, http.MethodGet, "/playbackConfiguration/cfg", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags, _ := resp["tags"].(map[string]any)

	assert.Contains(t, tags, "new", "new tag must be present after update")
	assert.NotContains(t, tags, "old", "old tag must be removed on full tag replacement")
}

// ─────────────────────────────────────────────────────────────
// Gap 3: PutPlaybackConfiguration returns HlsConfiguration.ManifestEndpointPrefix
// ─────────────────────────────────────────────────────────────

func TestParity_PutPlaybackConfiguration_HlsConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "hls-cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	hlsCfg, ok := resp["HlsConfiguration"].(map[string]any)
	require.True(t, ok, "HlsConfiguration must be present in response")
	assert.NotEmpty(t, hlsCfg["ManifestEndpointPrefix"], "ManifestEndpointPrefix must be set")
	assert.Contains(t, hlsCfg["ManifestEndpointPrefix"], "hls-cfg", "ManifestEndpointPrefix must contain config name")
}

// ─────────────────────────────────────────────────────────────
// Gap 4: DeletePlaybackConfiguration is idempotent
// ─────────────────────────────────────────────────────────────

func TestParity_DeletePlaybackConfiguration_Idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{name: "delete_existing_returns_204", exists: true, wantStatus: http.StatusNoContent},
		{name: "delete_nonexistent_returns_204", exists: false, wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.exists {
				helperPutConfig(t, h, "cfg")
			}

			rec := doRequest(t, h, http.MethodDelete, "/playbackConfiguration/cfg", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 5: CreateChannel validates PlaybackMode
// ─────────────────────────────────────────────────────────────

func TestParity_CreateChannel_PlaybackModeValidation(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Gap 6: CreateChannel returns Tier field
// ─────────────────────────────────────────────────────────────

func TestParity_CreateChannel_ReturnsTier(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Gap 7: StartChannel and StopChannel are idempotent
// ─────────────────────────────────────────────────────────────

func TestParity_Channel_StartStopIdempotent(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Gap 8: UntagResource is idempotent for unknown ARNs
// ─────────────────────────────────────────────────────────────

func TestParity_UntagResource_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	unknownARN := "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/no-such-thing"

	rec := doRequestWithQuery(t, h, http.MethodDelete, "/tags/"+unknownARN, "tagKeys=any-key")
	assert.Equal(t, http.StatusNoContent, rec.Code, "untag on unknown ARN must be idempotent")
}

// ─────────────────────────────────────────────────────────────
// Gap 9: DeleteSourceLocation rejects when vod/live sources attached
// ─────────────────────────────────────────────────────────────

func TestParity_DeleteSourceLocation_WithAttachedSources(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		setup      func(t *testing.T, h *mediatailor.Handler)
		wantStatus int
	}{
		{
			name:       "no_attached_sources_ok",
			setup:      func(_ *testing.T, _ *mediatailor.Handler) {},
			wantStatus: http.StatusOK,
		},
		{
			name: "attached_vod_source_rejected",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "attached_live_source_rejected",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
				"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
			})

			tt.setup(t, h)

			rec := doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 10: CreatePrefetchSchedule stores and returns Retrieval/Consumption
// ─────────────────────────────────────────────────────────────

func TestParity_CreatePrefetchSchedule_RetrievalAndConsumption(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	helperPutConfig(t, h, "pc1")

	// StartTime/EndTime are unixTimestamp shapes on the wire (JSON number of
	// seconds since epoch), not RFC3339 strings — confirmed against
	// aws-sdk-go-v2's (de)serializers and botocore's service-2.json.
	const (
		retrievalStart  float64 = 1_767_225_600 // 2026-01-01T00:00:00Z
		retrievalEnd    float64 = 1_767_229_200 // 2026-01-01T01:00:00Z
		consumptionEnd  float64 = 1_767_232_800 // 2026-01-01T02:00:00Z
		consumptionSame         = retrievalEnd
	)

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", map[string]any{
		"Retrieval": map[string]any{
			"StartTime": retrievalStart,
			"EndTime":   retrievalEnd,
		},
		"Consumption": map[string]any{
			"StartTime": consumptionSame,
			"EndTime":   consumptionEnd,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	retrieval, ok := resp["Retrieval"].(map[string]any)
	require.True(t, ok, "Retrieval must be present in response")
	assert.InEpsilon(t, retrievalStart, retrieval["StartTime"], 0.001)
	assert.InEpsilon(t, retrievalEnd, retrieval["EndTime"], 0.001)

	consumption, ok := resp["Consumption"].(map[string]any)
	require.True(t, ok, "Consumption must be present in response")
	assert.InEpsilon(t, consumptionSame, consumption["StartTime"], 0.001)
	assert.InEpsilon(t, consumptionEnd, consumption["EndTime"], 0.001)
}

// ─────────────────────────────────────────────────────────────
// Gap 11: CreateChannel accepts and returns FillerSlate
// ─────────────────────────────────────────────────────────────

func TestParity_CreateChannel_FillerSlate(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Gap 12: Snapshot/Restore persists live sources, prefetch schedules, programs
// ─────────────────────────────────────────────────────────────

func TestParity_SnapshotRestorePersistsAllResources(t *testing.T) {
	t.Parallel()

	b1 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b1.CreateSourceLocation("sl1", "https://example.com", nil)
	require.NoError(t, err)

	_, err = b1.CreateLiveSource("sl1", "ls1", nil, nil)
	require.NoError(t, err)

	_, err = b1.PutPlaybackConfiguration("pc1", "https://ads.com", "https://video.com", nil)
	require.NoError(t, err)

	_, err = b1.CreatePrefetchSchedule("pc1", "sched1", nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateChannel("ch1", "LOOP", nil, nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateProgram("ch1", "prog1", "sl1", "", "ls1", nil)
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())

	b2 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	ls, err := b2.DescribeLiveSource("sl1", "ls1")
	require.NoError(t, err)
	assert.Equal(t, "ls1", ls.LiveSourceName)

	sched, err := b2.GetPrefetchSchedule("pc1", "sched1")
	require.NoError(t, err)
	assert.Equal(t, "sched1", sched.Name)

	prog, err := b2.DescribeProgram("ch1", "prog1")
	require.NoError(t, err)
	assert.Equal(t, "prog1", prog.ProgramName)
}
