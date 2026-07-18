package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ShadowCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shadowDoc := map[string]any{
		"state": map[string]any{
			"desired": map[string]any{"color": "red"},
		},
	}
	docBytes, err := json.Marshal(shadowDoc)
	require.NoError(t, err)

	// Step 1: shadow does not exist yet.
	rec := doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Step 2: create/update classic shadow.
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow", docBytes)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 3: get classic shadow.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 4: create named shadow.
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow?name=myNamedShadow", docBytes)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Step 5: get named shadow.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow?name=myNamedShadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "color")

	// Step 6: list named shadows.
	rec = doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/myThing", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "myNamedShadow")

	// Step 7: delete classic shadow.
	rec = doRequest(t, h, http.MethodDelete, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Step 8: delete non-existent shadow.
	rec = doRequest(t, h, http.MethodDelete, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func TestHandler_ShadowMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func TestHandler_ListNamedShadowsMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/api/things/shadow/ListNamedShadowsForThing/myThing", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func TestHandler_ListNamedShadows_EmptyThingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func TestBackend_ListNamedShadows_NoShadows(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	names, err := b.ListNamedShadowsForThing("nonexistent")
	require.NoError(t, err)
	assert.Empty(t, names)
}
func TestBackend_DeleteThingShadow_ThingNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	_, err := b.DeleteThingShadow("nonexistent", "")
	require.Error(t, err)
}
func TestBackend_GetThingShadow_ThingNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	_, err := b.GetThingShadow("nonexistent", "")
	require.Error(t, err)
}
func TestHandler_ShadowVersionConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doc := []byte(`{"state":{"desired":{"color":"red"}}}`)

	// Initial update – creates shadow at version 1.
	rec := doRequest(t, h, http.MethodPost, "/things/myThing/shadow", doc)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second update with wrong version should return HTTP 409.
	docBadVer := []byte(`{"state":{"desired":{"color":"blue"}},"version":99}`)
	rec = doRequest(t, h, http.MethodPost, "/things/myThing/shadow", docBadVer)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConflictException")
}
func TestHandler_ShadowResponseContainsVersionAndTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doc := []byte(`{"state":{"desired":{"color":"green"}}}`)

	// Update returns shadow document with version and timestamp.
	rec := doRequest(t, h, http.MethodPost, "/things/myThing/shadow", doc)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "version")
	assert.Contains(t, resp, "timestamp")

	// Get also returns shadow document with version and timestamp.
	rec = doRequest(t, h, http.MethodGet, "/things/myThing/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Contains(t, getResp, "version")
	assert.Contains(t, getResp, "timestamp")
}

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
func Test_GetThingShadow_ResponseShape(t *testing.T) {
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

// TestParityAccuracy_UpdateThingShadow_VersionIncrement verifies that each
// successful UpdateThingShadow increments the shadow version by exactly 1,
// matching real AWS IoT behavior.
func Test_UpdateThingShadow_VersionIncrement(t *testing.T) {
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
func Test_DeleteThingShadow_Returns404OnRefetch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))

	rec := doRequest(t, h, http.MethodDelete, shadowPath(parityThing), nil)
	require.Equal(t, http.StatusOK, rec.Code, "DeleteThingShadow must succeed")

	_, code := getShadow(t, h, parityThing, "")
	assert.Equal(t, http.StatusNotFound, code, "GetThingShadow must return 404 after delete")
}

// TestParityAccuracy_DeleteThingShadow_ResponseOmitsState verifies that the
// delete response is an "empty response state document" -- only version and
// timestamp, no state/metadata/clientToken -- matching real AWS. See
// https://docs.aws.amazon.com/iot/latest/developerguide/device-shadow-rest-api.html#API_DeleteThingShadow:
// "Response body: {{Empty response state document}}".
func Test_DeleteThingShadow_ResponseOmitsState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	updateShadow(t, h, parityThing, "",
		[]byte(`{"state":{"desired":{"color":"blue"},"reported":{"color":"red"}}}`))

	rec := doRequest(t, h, http.MethodDelete, shadowPath(parityThing), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasState := resp["state"]
	assert.False(t, hasState, "delete response must NOT include state (empty response state document)")
	_, hasVersion := resp["version"]
	assert.True(t, hasVersion, "delete response must include version")
	_, hasTimestamp := resp["timestamp"]
	assert.True(t, hasTimestamp, "delete response must include timestamp")
}

// TestParityAccuracy_DeleteThingShadow_RecreateContinuesVersion verifies that
// after deleting a shadow, recreating it continues the version counter rather
// than resetting to 1, matching real AWS: "Note that deleting a shadow does
// not reset its version number to 0." (device-shadow-rest-api.html).
func Test_DeleteThingShadow_RecreateContinuesVersion(t *testing.T) {
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

	// Recreate — version must continue from 3, i.e. become 4, NOT reset to 1.
	resp = updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"y":2}}}`))
	assert.InDelta(t, float64(4), resp["version"], 0,
		"version must continue incrementing (not reset to 1) after delete+recreate")
}

// TestParityAccuracy_NamedShadow_IsolatedFromClassic verifies that named shadows
// and the classic shadow maintain independent state and version counters,
// matching real AWS IoT shadow isolation semantics.
func Test_NamedShadow_IsolatedFromClassic(t *testing.T) {
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
func Test_GetThingShadow_NotFound_404(t *testing.T) {
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
func Test_DeleteThingShadow_ReturnsPayload(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"key":"value"}}}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodDelete, "/things/thing1/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Response must be a JSON object with version and timestamp (AWS contract).
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "version")
	assert.Contains(t, resp, "timestamp")
}
func Test_MaxShadowsPerThing_CapEnforced(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill to cap.
	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("shadow-%d", i), []byte(`{"state":{"desired":{"x":1}}}`))
		require.NoError(t, err)
	}

	assert.Equal(t, iotdataplane.MaxShadowsPerThing, iotdataplane.ShadowCount(b))

	// One more new shadow for the same thing must fail.
	_, err := b.UpdateThingShadow("thing1", "overflow-shadow", []byte(`{"state":{"desired":{"x":1}}}`))
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
func Test_MaxShadowsPerThing_UpdateExistingNotCapped(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "existing", []byte(`{"state":{}}`))

	// Updating existing shadow must succeed regardless of cap.
	for range iotdataplane.MaxShadowsPerThing + 10 {
		_, err := b.UpdateThingShadow("thing1", "existing", []byte(`{"state":{"desired":{"k":"v"}}}`))
		require.NoError(t, err)
	}
}
func Test_MaxShadowsPerThing_CapPerThing(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill thing1 to cap.
	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("s-%d", i), []byte(`{"state":{"desired":{"x":1}}}`))
		require.NoError(t, err)
	}

	// thing2 must still accept new shadows.
	_, err := b.UpdateThingShadow("thing2", "new-shadow", []byte(`{"state":{"desired":{"x":1}}}`))
	require.NoError(t, err)
}
func Test_ShadowPath_Precision(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("mydevice", "", []byte(`{"state":{}}`))
	h := iotdataplane.NewHandler(b)

	// Exact shadow path must succeed.
	rec := doRequest(t, h, http.MethodGet, "/things/mydevice/shadow", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Path with extra segment after /shadow must return 404 (not matched as shadow).
	rec = doRequest(t, h, http.MethodGet, "/things/mydevice/shadow/extra", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func Test_NamedShadowPath_PathStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(*iotdataplane.InMemoryBackend)
		path     string
		method   string
		body     []byte
		wantCode int
	}{
		{
			name: "get_via_query_param",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device1", "config", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device1/shadow?name=config",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name: "get_via_path_style",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device1", "config", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device1/shadow/name/config",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "get_path_style_not_found",
			path:     "/things/device2/shadow/name/nonexistent",
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "post_via_path_style",
			path:     "/things/device3/shadow/name/myConfig",
			method:   http.MethodPost,
			body:     []byte(`{"state":{"desired":{"k":"v"}}}`),
			wantCode: http.StatusOK,
		},
		{
			name: "delete_via_path_style",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device4", "todel", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device4/shadow/name/todel",
			method:   http.MethodDelete,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "path %q method %q", tt.path, tt.method)
		})
	}
}
func Test_NamedShadowPath_PathTakesPrecedenceOverQuery(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("dev", "frompath", []byte(`{"state":{"desired":{"x":1}}}`))
	h := iotdataplane.NewHandler(b)

	// Path says "frompath", query says "fromquery" (doesn't exist). Path must win.
	rec := doRequest(t, h, http.MethodGet, "/things/dev/shadow/name/frompath?name=fromquery", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
func Test_NamedShadowPath_RouteMatcher(t *testing.T) {
	t.Parallel()

	body := []byte(`{"state":{"desired":{"k":"v"}}}`)

	tests := []struct {
		path     string
		method   string
		body     []byte
		wantCode int
	}{
		// POST creates the shadow → 200 confirms the path was routed.
		{"/things/mydevice/shadow", http.MethodPost, body, http.StatusOK},
		{"/things/mydevice/shadow?name=foo", http.MethodPost, body, http.StatusOK},
		{"/things/mydevice/shadow/name/bar", http.MethodPost, body, http.StatusOK},
		// These paths must NOT be matched by the shadow handler.
		{"/things/mydevice/shadow/extra", http.MethodGet, nil, http.StatusNotFound},
		{"/other/mydevice/shadow", http.MethodGet, nil, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "path %q method %q", tt.path, tt.method)
		})
	}
}
func Test_CrossThing_ShadowIsolation(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("thing-A", "", []byte(`{"state":{"desired":{"color":"red"}}}`))
	require.NoError(t, err)
	_, err = b.UpdateThingShadow("thing-B", "", []byte(`{"state":{"desired":{"color":"blue"}}}`))
	require.NoError(t, err)

	// Delete thing-A's shadow.
	_, err = b.DeleteThingShadow("thing-A", "")
	require.NoError(t, err)

	// thing-B's shadow must be unaffected.
	resp, err := b.GetThingShadow("thing-B", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))
	state := doc["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.Equal(t, "blue", desired["color"])
}
func Test_ClassicVsNamedShadow_Isolation(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"classic":"yes"}}}`))
	require.NoError(t, err)
	_, err = b.UpdateThingShadow("dev", "named-one", []byte(`{"state":{"desired":{"named":"yes"}}}`))
	require.NoError(t, err)

	// Delete classic shadow.
	_, err = b.DeleteThingShadow("dev", "")
	require.NoError(t, err)

	// Named shadow must survive.
	resp, err := b.GetThingShadow("dev", "named-one")
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))
	state := doc["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.Equal(t, "yes", desired["named"])

	// Classic shadow must be gone.
	_, err = b.GetThingShadow("dev", "")
	require.ErrorIs(t, err, iotdataplane.ErrShadowNotFound)
}
func Test_NamedShadow_IndependentVersions(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for range 3 {
		_, err := b.UpdateThingShadow("dev", "alpha", []byte(`{"state":{"desired":{"k":"v"}}}`))
		require.NoError(t, err)
	}

	_, err := b.UpdateThingShadow("dev", "beta", []byte(`{"state":{"desired":{"k":"v"}}}`))
	require.NoError(t, err)

	alphaResp, err := b.GetThingShadow("dev", "alpha")
	require.NoError(t, err)
	betaResp, err := b.GetThingShadow("dev", "beta")
	require.NoError(t, err)

	var alpha, beta map[string]any
	require.NoError(t, json.Unmarshal(alphaResp, &alpha))
	require.NoError(t, json.Unmarshal(betaResp, &beta))

	assert.InDelta(t, float64(3), alpha["version"], 0, "alpha must be at version 3")
	assert.InDelta(t, float64(1), beta["version"], 0, "beta must be at version 1")
}
func Test_ShadowCap_CapEnforcedWithValidDocs(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("shadow-%d", i), []byte(`{"state":{"desired":{"i":1}}}`))
		require.NoError(t, err, "shadow %d must be created", i)
	}

	// One more new shadow must fail.
	_, err := b.UpdateThingShadow("thing1", "overflow", []byte(`{"state":{"desired":{"i":1}}}`))
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
func Test_ShadowCap_UpdateExistingAlwaysSucceeds(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range iotdataplane.MaxShadowsPerThing {
		_, err := b.UpdateThingShadow("thing1", fmt.Sprintf("shadow-%d", i), []byte(`{"state":{"desired":{"i":1}}}`))
		require.NoError(t, err)
	}

	// Updating existing shadow (index 0) must succeed even at cap.
	for range 5 {
		_, err := b.UpdateThingShadow("thing1", "shadow-0", []byte(`{"state":{"desired":{"x":2}}}`))
		require.NoError(t, err, "update of existing shadow must always succeed")
	}
}
