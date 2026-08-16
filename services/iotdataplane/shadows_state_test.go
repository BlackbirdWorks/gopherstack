package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func Test_ShadowDesiredNull_WipesDesired(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Set desired with several keys.
	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72,"fan":"on","mode":"cool"}}}`))
	require.NoError(t, err)

	// Wipe desired section with explicit null.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	// GET response must not include desired section.
	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must be absent after null wipe")
}
func Test_ShadowDesiredNull_LeavesReportedIntact(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72},"reported":{"sensor":25}}}`))
	require.NoError(t, err)

	// Wipe only desired.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must be absent after null wipe")

	reported, hasReported := state["reported"].(map[string]any)
	require.True(t, hasReported, "reported must still be present")
	assert.InDelta(t, float64(25), reported["sensor"], 0)
}
func Test_ShadowReportedNull_WipesReported(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(
		`{"state":{"desired":{"mode":"cool"},"reported":{"temp":72,"fan":"on"}}}`))
	require.NoError(t, err)

	// Wipe reported section.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"reported":null}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasReported := state["reported"]
	assert.False(t, hasReported, "reported must be absent after null wipe")

	// Desired must still be present.
	desired, hasDesired := state["desired"].(map[string]any)
	require.True(t, hasDesired, "desired must still be present")
	assert.Equal(t, "cool", desired["mode"])
}
func Test_ShadowBothNull_WipesBoth(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"},"reported":{"k":"v"}}}`))
	require.NoError(t, err)

	// Wipe both sections simultaneously.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null,"reported":null}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDesired := state["desired"]
	_, hasReported := state["reported"]
	assert.False(t, hasDesired, "desired must be absent after null wipe")
	assert.False(t, hasReported, "reported must be absent after null wipe")
}
func Test_ShadowDesiredNull_ThenResetDesired(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72}}}`))
	require.NoError(t, err)

	// Wipe desired.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	// Re-set desired.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":65}}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.InDelta(t, float64(65), desired["temp"], 0)
	// Old key from before the wipe must not reappear.
	_, hasFan := desired["fan"]
	assert.False(t, hasFan)
}
func Test_ShadowDesiredNull_VersionStillIncrements(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	resp1, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	require.NoError(t, err)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(resp1, &r1))

	resp2, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(resp2, &r2))

	v1 := int(r1["version"].(float64))
	v2 := int(r2["version"].(float64))
	assert.Equal(t, v1+1, v2, "version must increment even on null wipe")
}
func Test_ShadowDesiredNull_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev/shadow", []byte(`{"state":{"desired":{"led":"on"}}}`))

	// Wipe desired via HTTP.
	rec := doRequest(t, h, http.MethodPost, "/things/dev/shadow", []byte(`{"state":{"desired":null}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	// Confirm desired absent in GET.
	rec = doRequest(t, h, http.MethodGet, "/things/dev/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must be absent after null wipe")
}
func Test_ShadowResponse_OnlyDesiredSet_ReportedOmitted(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72}}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDesired := state["desired"]
	assert.True(t, hasDesired, "desired must be present")
	_, hasReported := state["reported"]
	assert.False(t, hasReported, "reported must be absent when never set")
}
func Test_ShadowResponse_OnlyReportedSet_DesiredOmitted(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"reported":{"temp":72}}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasReported := state["reported"]
	assert.True(t, hasReported, "reported must be present")
	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must be absent when never set")
}
func Test_ShadowResponse_BothSet_BothPresent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"a":1},"reported":{"b":2}}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDesired := state["desired"]
	_, hasReported := state["reported"]
	assert.True(t, hasDesired, "desired must be present")
	assert.True(t, hasReported, "reported must be present")
}
func Test_ShadowResponse_EmptyStateSection_StateObjectPresent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// After wiping both sections, shadow still exists with empty state.
	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	require.NoError(t, err)
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	// "state" key must still exist (as empty object {}).
	_, hasState := doc["state"]
	assert.True(t, hasState, "state section must be present even when empty")
	_, hasVersion := doc["version"]
	assert.True(t, hasVersion, "version must always be present")
}
func Test_ShadowMetadata_ClearedOnDesiredNull(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Set desired to populate metadata.
	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72}}}`))
	require.NoError(t, err)

	// Wipe desired.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	// Set reported to ensure metadata section exists but only for reported.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"reported":{"sensor":25}}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	meta, hasMeta := doc["metadata"].(map[string]any)
	if hasMeta {
		// desired metadata must not be present.
		_, hasDesiredMeta := meta["desired"]
		assert.False(t, hasDesiredMeta, "desired metadata must be absent after null wipe")
		// reported metadata must still be present.
		_, hasReportedMeta := meta["reported"]
		assert.True(t, hasReportedMeta, "reported metadata must be present")
	}
}
func Test_ShadowDelta_AfterDesiredWipe_NoDelta(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72},"reported":{"temp":68}}}`))
	require.NoError(t, err)

	// Wipe desired — no more delta possible.
	_, err = b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	_, hasDelta := state["delta"]
	assert.False(t, hasDelta, "delta must be absent when desired is wiped")
}
func Test_ShadowUpdate_EmptyDesiredPatch_NoOpForKeys(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"temp":72,"fan":"on"}}}`))
	require.NoError(t, err)

	// Empty desired patch — keys preserved, version still increments.
	resp2, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{}}}`))
	require.NoError(t, err)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(resp2, &r2))
	assert.InDelta(t, float64(2), r2["version"], 0)

	state := r2["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.InDelta(t, float64(72), desired["temp"], 0, "existing key must survive empty patch")
	assert.Equal(t, "on", desired["fan"])
}
func Test_ShadowUpdate_MultiplePatchesAccumulate(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	updates := []string{
		`{"state":{"desired":{"a":1}}}`,
		`{"state":{"desired":{"b":2}}}`,
		`{"state":{"desired":{"c":3}}}`,
		`{"state":{"reported":{"a":1}}}`,
	}

	for _, u := range updates {
		_, err := b.UpdateThingShadow("dev", "", []byte(u))
		require.NoError(t, err)
	}

	resp, err := b.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))

	state := doc["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.InDelta(t, float64(1), desired["a"], 0)
	assert.InDelta(t, float64(2), desired["b"], 0)
	assert.InDelta(t, float64(3), desired["c"], 0)
	assert.InDelta(t, float64(4), doc["version"], 0)
}
func Test_ShadowVersionLock_WithStateRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		body    string
	}{
		{
			name:    "correct_version_succeeds",
			body:    `{"state":{"desired":{"k":"v2"}},"version":1}`,
			wantErr: nil,
		},
		{
			name:    "wrong_version_conflicts",
			body:    `{"state":{"desired":{"k":"v2"}},"version":99}`,
			wantErr: iotdataplane.ErrVersionConflict,
		},
		{
			name:    "missing_state_with_version",
			body:    `{"version":1}`,
			wantErr: iotdataplane.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
			require.NoError(t, err)

			_, err = b.UpdateThingShadow("dev", "", []byte(tt.body))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
