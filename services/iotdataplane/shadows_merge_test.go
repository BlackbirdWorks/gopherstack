package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_ShadowVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updates     [][]byte
		wantVersion int
		wantErr     bool
	}{
		{
			name: "first_update_is_version_1",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
			},
			wantVersion: 1,
		},
		{
			name: "second_update_increments_to_2",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				[]byte(`{"state":{"desired":{"color":"blue"}}}`),
			},
			wantVersion: 2,
		},
		{
			name: "correct_version_check_succeeds",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				// second update supplies version=1 which matches current
				[]byte(`{"state":{"desired":{"color":"blue"}},"version":1}`),
			},
			wantVersion: 2,
		},
		{
			name: "wrong_version_check_returns_conflict",
			updates: [][]byte{
				[]byte(`{"state":{"desired":{"color":"red"}}}`),
				// second update supplies wrong version=99
				[]byte(`{"state":{"desired":{"color":"blue"}},"version":99}`),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()

			var lastErr error

			for _, doc := range tt.updates {
				_, lastErr = b.UpdateThingShadow("thing", "", doc)
				if lastErr != nil {
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				require.ErrorIs(t, lastErr, iotdataplane.ErrVersionConflict)

				return
			}

			require.NoError(t, lastErr)

			resp, err := b.GetThingShadow("thing", "")
			require.NoError(t, err)

			var result map[string]any
			require.NoError(t, json.Unmarshal(resp, &result))

			gotVersion, ok := result["version"].(float64)
			require.True(t, ok, "response should contain numeric version field")
			assert.Equal(t, tt.wantVersion, int(gotVersion))

			_, hasTimestamp := result["timestamp"]
			assert.True(t, hasTimestamp, "response should contain timestamp field")
		})
	}
}

// TestParityAccuracy_UpdateThingShadow_NullFieldDeletion verifies that setting
// a field to null in state.desired removes it from the shadow, matching real AWS
// IoT Data Plane merge-patch semantics.
func Test_UpdateThingShadow_NullFieldDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initial     []byte
		patch       []byte
		section     string
		wantAbsent  []string
		wantPresent []string
	}{
		{
			name:        "null_deletes_desired_key",
			initial:     []byte(`{"state":{"desired":{"color":"red","brightness":100}}}`),
			patch:       []byte(`{"state":{"desired":{"color":null}}}`),
			section:     "desired",
			wantAbsent:  []string{"color"},
			wantPresent: []string{"brightness"},
		},
		{
			name:        "null_deletes_reported_key",
			initial:     []byte(`{"state":{"reported":{"temp":22,"humidity":60}}}`),
			patch:       []byte(`{"state":{"reported":{"humidity":null}}}`),
			section:     "reported",
			wantAbsent:  []string{"humidity"},
			wantPresent: []string{"temp"},
		},
		{
			name:        "non_null_keys_preserved",
			initial:     []byte(`{"state":{"desired":{"a":"1","b":"2","c":"3"}}}`),
			patch:       []byte(`{"state":{"desired":{"b":null}}}`),
			section:     "desired",
			wantAbsent:  []string{"b"},
			wantPresent: []string{"a", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			updateShadow(t, h, parityThing, "", tt.initial)
			updateShadow(t, h, parityThing, "", tt.patch)

			resp, code := getShadow(t, h, parityThing, "")
			require.Equal(t, http.StatusOK, code)

			state, ok := resp["state"].(map[string]any)
			require.True(t, ok, "state must be an object")

			section, ok := state[tt.section].(map[string]any)
			require.True(t, ok, "state.%s must be an object", tt.section)

			for _, key := range tt.wantAbsent {
				_, present := section[key]
				assert.False(t, present, "key %q must be absent from state.%s after null patch", key, tt.section)
			}

			for _, key := range tt.wantPresent {
				_, present := section[key]
				assert.True(t, present, "key %q must remain in state.%s", key, tt.section)
			}
		})
	}
}

// TestParityAccuracy_ShadowDelta_PreciseFields verifies that the delta in a shadow
// response contains exactly the desired fields that differ from reported, matching
// real AWS IoT Data Plane delta computation semantics.
func Test_ShadowDelta_PreciseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		doc         []byte
		wantDelta   []string
		noDeltaKeys []string
	}{
		{
			name:      "all_desired_differs",
			doc:       []byte(`{"state":{"desired":{"a":1,"b":2},"reported":{}}}`),
			wantDelta: []string{"a", "b"},
		},
		{
			name: "partial_match",
			doc:  []byte(`{"state":{"desired":{"a":1,"b":2},"reported":{"a":1,"b":99}}}`),
			// "a" matches, "b" differs → delta should have only "b"
			wantDelta:   []string{"b"},
			noDeltaKeys: []string{"a"},
		},
		{
			name: "all_match_no_delta",
			doc:  []byte(`{"state":{"desired":{"x":5},"reported":{"x":5}}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			updateShadow(t, h, parityThing, "", tt.doc)

			resp, code := getShadow(t, h, parityThing, "")
			require.Equal(t, http.StatusOK, code)

			if len(tt.wantDelta) == 0 && len(tt.noDeltaKeys) == 0 {
				state, _ := resp["state"].(map[string]any)
				_, hasDelta := state["delta"]
				assert.False(t, hasDelta, "no delta expected when desired == reported")

				return
			}

			state, ok := resp["state"].(map[string]any)
			require.True(t, ok)

			delta, ok := state["delta"].(map[string]any)
			require.True(t, ok, "delta must be present when desired != reported")

			for _, key := range tt.wantDelta {
				_, exists := delta[key]
				assert.True(t, exists, "key %q must be in delta", key)
			}

			for _, key := range tt.noDeltaKeys {
				_, exists := delta[key]
				assert.False(t, exists, "key %q must NOT be in delta (values match)", key)
			}
		})
	}
}
func Test_ShadowVersion_OverflowProtection(t *testing.T) {
	t.Parallel()

	// Create a shadow with version just below the rollover point, then update
	// to verify it resets to 1 rather than overflowing into negative territory.
	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{}`))

	// Force the version to max by doing many updates. We'll set it via the internal
	// helper and verify the next update starts a new cycle.
	iotdataplane.ForceSetShadowVersion(b, "thing1", "", iotdataplane.MaxShadowVersion)

	resp, err := b.UpdateThingShadow("thing1", "", []byte(`{"state":{}}`))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp, &result))

	versionFloat, ok := result["version"].(float64)
	require.True(t, ok, "version must be a number")
	assert.Equal(t, 1, int(versionFloat), "version must reset to 1 after overflow")
}
func Test_ShadowMerge_DesiredPartialUpdate(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev1/shadow", []byte(`{
		"state": {"desired": {"temp": 68, "fan": "on"}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev1/shadow", []byte(`{
		"state": {"desired": {"fan": "off"}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev1/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.InDelta(t, float64(68), desired["temp"], 0, "temp must be preserved after partial update")
	assert.Equal(t, "off", desired["fan"])
}
func Test_ShadowMerge_NullDeletion(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev2/shadow", []byte(`{
		"state": {"desired": {"keep": "yes", "remove": "bye"}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev2/shadow", []byte(`{
		"state": {"desired": {"remove": null}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev2/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.Equal(t, "yes", desired["keep"])
	_, hasRemove := desired["remove"]
	assert.False(t, hasRemove, "null-deleted key must not appear in response")
}
func Test_ShadowMerge_ReportedIndependent(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev3/shadow", []byte(`{
		"state": {"desired": {"target": 72}, "reported": {"current": 68}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev3/shadow", []byte(`{
		"state": {"reported": {"current": 72}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev3/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	reported := state["reported"].(map[string]any)
	assert.InDelta(t, float64(72), desired["target"], 0, "desired must be unchanged")
	assert.InDelta(t, float64(72), reported["current"], 0)
}
func Test_ShadowMerge_VersionIncrements(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	rec1 := doRequest(t, h, http.MethodPost, "/things/dev-ver/shadow",
		[]byte(`{"state":{"desired":{"k":"v1"}}}`))
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))

	rec2 := doRequest(t, h, http.MethodPost, "/things/dev-ver/shadow",
		[]byte(`{"state":{"desired":{"k":"v2"}}}`))
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))

	v1 := r1["version"].(float64)
	v2 := r2["version"].(float64)
	assert.InDelta(t, v1+1, v2, 0, "version must increment on each update")
}
func Test_ShadowDelta_ComputedOnGet(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-delta/shadow", []byte(`{
		"state": {"desired": {"target": 72}, "reported": {"current": 68}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-delta/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	delta, hasDelta := state["delta"]
	require.True(t, hasDelta, "delta must be present when desired != reported")

	deltaMap := delta.(map[string]any)
	assert.InDelta(t, float64(72), deltaMap["target"], 0)
	_, hasUnrelated := deltaMap["current"]
	assert.False(t, hasUnrelated, "delta must not contain reported-only keys")
}
func Test_ShadowDelta_AbsentWhenEqual(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-nodelta/shadow", []byte(`{
		"state": {"desired": {"temp": 70}, "reported": {"temp": 70}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-nodelta/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	_, hasDelta := state["delta"]
	assert.False(t, hasDelta, "delta must be absent when desired equals reported")
}
func Test_ShadowDelta_UpdatedAfterReported(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Initially desired=72, reported missing → delta contains target.
	doRequest(t, h, http.MethodPost, "/things/dev-delta2/shadow", []byte(`{
		"state": {"desired": {"target": 72}}
	}`))

	// Device reports it reached the target.
	doRequest(t, h, http.MethodPost, "/things/dev-delta2/shadow", []byte(`{
		"state": {"reported": {"target": 72}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-delta2/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	_, hasDelta := state["delta"]
	assert.False(t, hasDelta, "delta must clear once reported matches desired")
}
func Test_ShadowMetadata_PresentAfterUpdate(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-meta/shadow", []byte(`{
		"state": {"desired": {"temp": 72}, "reported": {"current": 70}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-meta/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	meta, hasMeta := resp["metadata"]
	require.True(t, hasMeta, "metadata must be present after update")

	metaMap := meta.(map[string]any)
	desiredMeta := metaMap["desired"].(map[string]any)
	tempMeta := desiredMeta["temp"].(map[string]any)
	ts, hasTS := tempMeta["timestamp"]
	assert.True(t, hasTS, "metadata.desired.temp.timestamp must be present")
	assert.Greater(t, ts.(float64), float64(0))
}
func Test_ShadowMetadata_UnchangedFieldTimestampPreserved(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-meta2/shadow", []byte(`{
		"state": {"desired": {"a": 1, "b": 2}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-meta2/shadow", nil)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))
	ts1 := resp1["metadata"].(map[string]any)["desired"].(map[string]any)["a"].(map[string]any)["timestamp"].(float64)

	// Update only b.
	doRequest(t, h, http.MethodPost, "/things/dev-meta2/shadow", []byte(`{
		"state": {"desired": {"b": 99}}
	}`))

	rec = doRequest(t, h, http.MethodGet, "/things/dev-meta2/shadow", nil)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp2))
	ts2 := resp2["metadata"].(map[string]any)["desired"].(map[string]any)["a"].(map[string]any)["timestamp"].(float64)

	assert.InDelta(t, ts1, ts2, 0, "timestamp for unmodified field must not change")
}
func Test_ClientToken_Echoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantToken   string
		wantPresent bool
	}{
		{
			name:        "token_echoed",
			body:        `{"state":{"desired":{"k":"v"}},"clientToken":"req-abc-123"}`,
			wantToken:   "req-abc-123",
			wantPresent: true,
		},
		{
			name:        "no_token_absent",
			body:        `{"state":{"desired":{"k":"v"}}}`,
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/things/dev-ct/shadow", []byte(tt.body))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			token, hasToken := resp["clientToken"]
			if tt.wantPresent {
				require.True(t, hasToken, "clientToken must be present in response")
				assert.Equal(t, tt.wantToken, token)
			} else {
				assert.False(t, hasToken, "clientToken must be absent when not provided")
			}
		})
	}
}
func Test_MergeStateFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     string
		patch    string
		wantKeys map[string]string
		deleted  []string
	}{
		{
			name:     "add_new_key",
			base:     `{"a":1}`,
			patch:    `{"b":2}`,
			wantKeys: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:     "delete_with_null",
			base:     `{"a":1,"b":2}`,
			patch:    `{"b":null}`,
			wantKeys: map[string]string{"a": "1"},
			deleted:  []string{"b"},
		},
		{
			name:     "update_existing",
			base:     `{"a":1}`,
			patch:    `{"a":99}`,
			wantKeys: map[string]string{"a": "99"},
		},
		{
			name:     "empty_patch_unchanged",
			base:     `{"a":1}`,
			patch:    `{}`,
			wantKeys: map[string]string{"a": "1"},
		},
		{
			name:     "nil_base_uses_patch",
			base:     `{}`,
			patch:    `{"x":42}`,
			wantKeys: map[string]string{"x": "42"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var base, patch map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tt.base), &base))
			require.NoError(t, json.Unmarshal([]byte(tt.patch), &patch))

			result := iotdataplane.MergeStateFields(base, patch)

			for k, wantVal := range tt.wantKeys {
				got, ok := result[k]
				require.True(t, ok, "key %q must be present", k)
				assert.Equal(t, wantVal, string(got))
			}

			for _, k := range tt.deleted {
				_, ok := result[k]
				assert.False(t, ok, "key %q must be deleted", k)
			}
		})
	}
}
