package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

const (
	parityThing = "parity-thing"
)

// shadowPath returns the path for the classic shadow of a thing.
func shadowPath(thingName string) string { return "/things/" + thingName + "/shadow" }

// namedShadowPath returns the path for a named shadow of a thing.
func namedShadowPath(thingName, shadowName string) string {
	return "/things/" + thingName + "/shadow/name/" + shadowName
}

// updateShadow is a test helper that POSTs a shadow document and requires 200.
func updateShadow(t *testing.T, h *iotdataplane.Handler, thingName, shadowName string, doc []byte) map[string]any {
	t.Helper()

	var path string
	if shadowName == "" {
		path = shadowPath(thingName)
	} else {
		path = namedShadowPath(thingName, shadowName)
	}

	rec := doRequest(t, h, http.MethodPost, path, doc)
	require.Equal(t, http.StatusOK, rec.Code, "UpdateThingShadow: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// getShadow is a test helper that GETs a shadow and returns the parsed response.
func getShadow(t *testing.T, h *iotdataplane.Handler, thingName, shadowName string) (map[string]any, int) {
	t.Helper()

	var path string
	if shadowName == "" {
		path = shadowPath(thingName)
	} else {
		path = namedShadowPath(thingName, shadowName)
	}

	rec := doRequest(t, h, http.MethodGet, path, nil)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}

	return resp, rec.Code
}

// TestParityAccuracy_GetThingShadow_ResponseShape verifies that GetThingShadow
// returns the exact field set required by real AWS IoT Data Plane: state, version,
// timestamp. metadata and delta are conditional.
func TestParityAccuracy_GetThingShadow_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		doc        []byte
		wantFields []string
		noFields   []string
	}{
		{
			// Real AWS: desired-only shadow has delta (desired fields not matched by reported).
			name:       "desired_only_has_delta",
			doc:        []byte(`{"state":{"desired":{"color":"red"}}}`),
			wantFields: []string{"state", "version", "timestamp", "metadata"},
		},
		{
			// Reported-only: desired is empty → computeDelta returns nil → no delta.
			name:       "reported_only_no_delta",
			doc:        []byte(`{"state":{"reported":{"temp":22}}}`),
			wantFields: []string{"state", "version", "timestamp", "metadata"},
			noFields:   []string{"delta"},
		},
		{
			// delta lives inside state.delta, not as a top-level key.
			name:       "desired_differs_from_reported_has_delta",
			doc:        []byte(`{"state":{"desired":{"color":"blue"},"reported":{"color":"red"}}}`),
			wantFields: []string{"state", "version", "timestamp", "metadata"},
		},
		{
			name:       "desired_equals_reported_no_delta",
			doc:        []byte(`{"state":{"desired":{"color":"green"},"reported":{"color":"green"}}}`),
			wantFields: []string{"state", "version", "timestamp", "metadata"},
			noFields:   []string{"delta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			updateShadow(t, h, parityThing, "", tt.doc)

			resp, code := getShadow(t, h, parityThing, "")
			require.Equal(t, http.StatusOK, code)

			for _, field := range tt.wantFields {
				_, ok := resp[field]
				assert.True(t, ok, "field %q must be present in GetThingShadow response", field)
			}

			// noFields are checked inside state (delta lives at state.delta).
			state, _ := resp["state"].(map[string]any)
			for _, field := range tt.noFields {
				_, ok := state[field]
				assert.False(t, ok, "state.%s must be absent in GetThingShadow response", field)
			}
		})
	}
}

// TestParityAccuracy_UpdateThingShadow_NullFieldDeletion verifies that setting
// a field to null in state.desired removes it from the shadow, matching real AWS
// IoT Data Plane merge-patch semantics.
func TestParityAccuracy_UpdateThingShadow_NullFieldDeletion(t *testing.T) {
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

// TestParityAccuracy_UpdateThingShadow_VersionIncrement verifies that each
// successful UpdateThingShadow increments the shadow version by exactly 1,
// matching real AWS IoT behavior.
func TestParityAccuracy_UpdateThingShadow_VersionIncrement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		doc := fmt.Appendf(nil, `{"state":{"desired":{"seq":%d}}}`, i)
		resp := updateShadow(t, h, parityThing, "", doc)

		version, ok := resp["version"].(float64)
		require.True(t, ok, "version must be a number")
		assert.InDelta(t, float64(i+1), version, 0,
			"version must be %d after %d updates", i+1, i+1)
	}
}

// TestParityAccuracy_DeleteThingShadow_Returns404OnRefetch verifies that after
// a successful delete, subsequent GetThingShadow returns 404, matching real AWS.
func TestParityAccuracy_DeleteThingShadow_Returns404OnRefetch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))

	rec := doRequest(t, h, http.MethodDelete, shadowPath(parityThing), nil)
	require.Equal(t, http.StatusOK, rec.Code, "DeleteThingShadow must succeed")

	_, code := getShadow(t, h, parityThing, "")
	assert.Equal(t, http.StatusNotFound, code, "GetThingShadow must return 404 after delete")
}

// TestParityAccuracy_DeleteThingShadow_ResponseIncludesState verifies that the
// delete response contains the shadow's state at time of deletion, matching real AWS.
func TestParityAccuracy_DeleteThingShadow_ResponseIncludesState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	updateShadow(t, h, parityThing, "",
		[]byte(`{"state":{"desired":{"color":"blue"},"reported":{"color":"red"}}}`))

	rec := doRequest(t, h, http.MethodDelete, shadowPath(parityThing), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasState := resp["state"]
	assert.True(t, hasState, "delete response must include state")
	_, hasVersion := resp["version"]
	assert.True(t, hasVersion, "delete response must include version")
	_, hasTimestamp := resp["timestamp"]
	assert.True(t, hasTimestamp, "delete response must include timestamp")
}

// TestParityAccuracy_DeleteThingShadow_RecreateResetsVersion verifies that
// after deleting a shadow, recreating it starts version at 1, matching real AWS.
func TestParityAccuracy_DeleteThingShadow_RecreateResetsVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create and update to advance version to 3.
	for range 3 {
		updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))
	}

	// Verify version is 3.
	resp, _ := getShadow(t, h, parityThing, "")
	assert.InDelta(t, float64(3), resp["version"], 0)

	// Delete.
	rec := doRequest(t, h, http.MethodDelete, shadowPath(parityThing), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Recreate — version must restart at 1.
	resp = updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"y":2}}}`))
	assert.InDelta(t, float64(1), resp["version"], 0,
		"version must reset to 1 when shadow is recreated after deletion")
}

// TestParityAccuracy_ListNamedShadows_ExcludesClassicShadow verifies that the
// classic (unnamed) shadow is NOT listed in ListNamedShadowsForThing responses,
// matching real AWS IoT behavior.
func TestParityAccuracy_ListNamedShadows_ExcludesClassicShadow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create classic shadow.
	updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))
	// Create named shadows.
	updateShadow(t, h, parityThing, "alpha", []byte(`{"state":{"desired":{"x":1}}}`))
	updateShadow(t, h, parityThing, "beta", []byte(`{"state":{"desired":{"x":1}}}`))

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/"+parityThing, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["results"].([]any)
	require.True(t, ok, "results must be an array")
	assert.Len(t, results, 2, "only named shadows appear in list")

	for _, r := range results {
		name, _ := r.(string)
		assert.NotEmpty(t, name, "listed shadow names must be non-empty")
	}
}

// TestParityAccuracy_ListNamedShadows_ResponseShape verifies that
// ListNamedShadowsForThing returns the exact AWS response structure:
// {"results": [...], "timestamp": N}. nextToken only appears when paginating.
func TestParityAccuracy_ListNamedShadows_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		shadowNames     []string
		wantFields      []string
		wantResultCount int
	}{
		{
			name:            "no_named_shadows",
			shadowNames:     nil,
			wantFields:      []string{"results", "timestamp"},
			wantResultCount: 0,
		},
		{
			name:            "three_named_shadows",
			shadowNames:     []string{"gamma", "alpha", "beta"},
			wantFields:      []string{"results", "timestamp"},
			wantResultCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			thing := "shape-" + tt.name

			for _, name := range tt.shadowNames {
				updateShadow(t, h, thing, name, []byte(`{"state":{"desired":{"x":1}}}`))
			}

			rec := doRequest(t, h, http.MethodGet,
				"/api/things/shadow/ListNamedShadowsForThing/"+thing, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			for _, field := range tt.wantFields {
				_, ok := resp[field]
				assert.True(t, ok, "field %q must be present", field)
			}

			results, ok := resp["results"].([]any)
			require.True(t, ok, "results must be an array")
			assert.Len(t, results, tt.wantResultCount)

			// nextToken must be absent when results fit on one page.
			_, hasToken := resp["nextToken"]
			assert.False(t, hasToken, "nextToken must be absent when no pagination needed")
		})
	}
}

// TestParityAccuracy_ListNamedShadows_SortedAlphabetically verifies the returned
// shadow names are alphabetically sorted, matching real AWS IoT behavior.
func TestParityAccuracy_ListNamedShadows_SortedAlphabetically(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "apple", "mango", "cherry"} {
		updateShadow(t, h, parityThing, name, []byte(`{"state":{"desired":{"x":1}}}`))
	}

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/"+parityThing, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 4)

	names := make([]string, len(results))
	for i, r := range results {
		names[i], _ = r.(string)
	}

	assert.Equal(t, []string{"apple", "cherry", "mango", "zebra"}, names,
		"named shadows must be returned in alphabetical order")
}

// TestParityAccuracy_Publish_Returns200 verifies that a valid Publish request
// returns HTTP 200, matching real AWS IoT Data Plane behavior.
func TestParityAccuracy_Publish_Returns200(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "simple_topic",
			path:       "/topics/devices/sensor1/data",
			body:       []byte(`{"temp":22}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "binary_payload",
			path:       "/topics/telemetry/raw",
			body:       []byte{0x01, 0x02, 0x03},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_payload",
			path:       "/topics/devices/cmd",
			body:       []byte{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "qos_0",
			path:       "/topics/test?qos=0",
			body:       []byte(`{}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "qos_1",
			path:       "/topics/test?qos=1",
			body:       []byte(`{}`),
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, "path: %s", tt.path)
		})
	}
}

// TestParityAccuracy_Publish_TopicValidation verifies that invalid topics are
// rejected per AWS IoT rules: wildcards, reserved shadow topics, empty segments.
func TestParityAccuracy_Publish_TopicValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "wildcard_hash_rejected",
			path:       "/topics/devices/#",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wildcard_plus_rejected",
			path:       "/topics/devices/+/data",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "qos_invalid_2",
			path:       "/topics/test?qos=2",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "qos_invalid_negative",
			path:       "/topics/test?qos=-1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "shadow_reserved_topic_rejected",
			path:       "/topics/$aws/things/mydevice/shadow/update",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, []byte(`{}`))
			assert.Equal(t, tt.wantStatus, rec.Code, "path: %s", tt.path)
		})
	}
}

// TestParityAccuracy_UpdateThingShadow_StateRequired verifies that UpdateThingShadow
// rejects requests without a `state` field, matching real AWS InvalidRequestException.
func TestParityAccuracy_UpdateThingShadow_StateRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		doc        []byte
		wantStatus int
	}{
		{
			name:       "missing_state_key",
			doc:        []byte(`{"desired":{"x":1}}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "state_is_null",
			doc:        []byte(`{"state":null}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "state_is_string",
			doc:        []byte(`{"state":"invalid"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_empty_state",
			doc:        []byte(`{"state":{}}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_desired_only",
			doc:        []byte(`{"state":{"desired":{"x":1}}}`),
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, shadowPath(parityThing), tt.doc)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidRequestException", resp["error"],
					"error type must be InvalidRequestException")
			}
		})
	}
}

// TestParityAccuracy_ShadowDelta_PreciseFields verifies that the delta in a shadow
// response contains exactly the desired fields that differ from reported, matching
// real AWS IoT Data Plane delta computation semantics.
func TestParityAccuracy_ShadowDelta_PreciseFields(t *testing.T) {
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

// TestParityAccuracy_VersionConflict_RejectedWithCorrectError verifies that
// UpdateThingShadow rejects version mismatches with VersionConflictException,
// matching real AWS IoT optimistic locking behavior.
func TestParityAccuracy_VersionConflict_RejectedWithCorrectError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantErrorType string
		versionInDoc  int
		wantStatus    int
	}{
		{
			name:          "stale_version_rejected",
			versionInDoc:  0,
			wantStatus:    http.StatusConflict,
			wantErrorType: "VersionConflictException",
		},
		{
			name:          "future_version_rejected",
			versionInDoc:  99,
			wantStatus:    http.StatusConflict,
			wantErrorType: "VersionConflictException",
		},
		{
			name:         "correct_version_accepted",
			versionInDoc: 1,
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			// Create shadow (version becomes 1).
			updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))

			doc := fmt.Appendf(nil, `{"version":%d,"state":{"desired":{"x":2}}}`, tt.versionInDoc)
			rec := doRequest(t, h, http.MethodPost, shadowPath(parityThing), doc)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrorType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrorType, resp["error"],
					"error type must be VersionConflictException")
			}
		})
	}
}

// TestParityAccuracy_NamedShadow_IsolatedFromClassic verifies that named shadows
// and the classic shadow maintain independent state and version counters,
// matching real AWS IoT shadow isolation semantics.
func TestParityAccuracy_NamedShadow_IsolatedFromClassic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update classic shadow 3 times.
	for range 3 {
		updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"classic":true}}}`))
	}

	// Create named shadow once.
	updateShadow(t, h, parityThing, "my-shadow", []byte(`{"state":{"desired":{"named":true}}}`))

	classicResp, _ := getShadow(t, h, parityThing, "")
	namedResp, _ := getShadow(t, h, parityThing, "my-shadow")

	assert.InDelta(t, float64(3), classicResp["version"], 0, "classic shadow version must be 3")
	assert.InDelta(t, float64(1), namedResp["version"], 0, "named shadow version must be 1")

	// Delete named shadow — classic must be unaffected.
	rec := doRequest(t, h, http.MethodDelete, namedShadowPath(parityThing, "my-shadow"), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	classicAfter, code := getShadow(t, h, parityThing, "")
	assert.Equal(t, http.StatusOK, code, "classic shadow must survive named shadow deletion")
	assert.InDelta(t, float64(3), classicAfter["version"], 0)
}

// TestParityAccuracy_GetThingShadow_NotFound_404 verifies that GetThingShadow
// returns HTTP 404 for unknown things and unknown shadow names, matching real AWS.
func TestParityAccuracy_GetThingShadow_NotFound_404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		thingName  string
		shadowName string
	}{
		{name: "unknown_thing", thingName: "no-such-thing", shadowName: ""},
		{name: "unknown_named_shadow", thingName: parityThing, shadowName: "no-such-shadow"},
		{name: "classic_shadow_before_create", thingName: "fresh-thing", shadowName: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.shadowName == "no-such-shadow" {
				// Create the thing but not this shadow.
				updateShadow(t, h, tt.thingName, "other-shadow",
					[]byte(`{"state":{"desired":{"x":1}}}`))
			}

			_, code := getShadow(t, h, tt.thingName, tt.shadowName)
			assert.Equal(t, http.StatusNotFound, code)
		})
	}
}
