package iotdataplane_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// ── Thing name validation ─────────────────────────────────────────────────────

func TestRefinement4_ValidateThingName_ValidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "alphanumeric", thingName: "device123"},
		{name: "with_colon", thingName: "arn:thing:device"},
		{name: "with_underscore", thingName: "my_device_01"},
		{name: "with_hyphen", thingName: "my-device-01"},
		{name: "with_dot", thingName: "device.sensor.v2"},
		{name: "max_length", thingName: strings.Repeat("a", iotdataplane.MaxThingNameLength)},
		{name: "single_char", thingName: "x"},
		{name: "mixed", thingName: "Device_01:sensor-v2.3"},
		{name: "all_valid_chars", thingName: "aZ0:_-."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateThingName(tt.thingName)
			assert.NoError(t, err, "thing name %q should be valid", tt.thingName)
		})
	}
}

func TestRefinement4_ValidateThingName_InvalidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "empty", thingName: ""},
		{name: "too_long", thingName: strings.Repeat("a", iotdataplane.MaxThingNameLength+1)},
		{name: "space", thingName: "device name"},
		{name: "slash", thingName: "device/sensor"},
		{name: "at_sign", thingName: "device@name"},
		{name: "hash", thingName: "device#1"},
		{name: "plus", thingName: "device+1"},
		{name: "asterisk", thingName: "device*"},
		{name: "bang", thingName: "device!"},
		{name: "dollar", thingName: "$system"},
		{name: "question_mark", thingName: "device?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateThingName(tt.thingName)
			require.ErrorIs(t, err, iotdataplane.ErrValidation, "thing name %q should be invalid", tt.thingName)
		})
	}
}

func TestRefinement4_ThingName_ValidationViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
		wantCode  int
	}{
		{
			name:      "valid_thing_name",
			thingName: "my-sensor-01",
			wantCode:  http.StatusOK,
		},
		{
			name:      "invalid_thing_name_space",
			thingName: "my%20sensor",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "invalid_thing_name_dollar",
			thingName: "$sys",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			path := "/things/" + tt.thingName + "/shadow"
			rec := doRequest(t, h, http.MethodPost, path, []byte(`{"state":{"desired":{"k":"v"}}}`))
			assert.Equal(t, tt.wantCode, rec.Code, "unexpected status for thing name %q", tt.thingName)
		})
	}
}

func TestRefinement4_ThingName_ValidationOnGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
		wantCode  int
	}{
		{name: "valid", thingName: "valid-thing", wantCode: http.StatusNotFound},
		// URL-encode space so httptest.NewRequest does not panic on invalid URL.
		// Go net/http decodes %20 back to space in r.URL.Path for validation.
		{name: "invalid_space", thingName: "invalid%20thing", wantCode: http.StatusBadRequest},
		// A slash in the path produces thingName "a/b" which fails the regex.
		{name: "invalid_slash", thingName: "a/b", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			path := "/things/" + tt.thingName + "/shadow"
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement4_ThingName_ValidationOnDelete(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	// URL-encode the space so httptest.NewRequest doesn't panic on an invalid URL.
	rec := doRequest(t, h, http.MethodDelete, "/things/bad%20name/shadow", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "error")
}

func TestRefinement4_ThingName_ValidationOnListNamedShadows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
		wantCode  int
	}{
		{name: "valid", thingName: "valid.thing", wantCode: http.StatusOK},
		{name: "invalid_bang", thingName: "bad!name", wantCode: http.StatusBadRequest},
		{
			name:      "too_long",
			thingName: strings.Repeat("x", iotdataplane.MaxThingNameLength+1),
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			path := "/api/things/shadow/ListNamedShadowsForThing/" + tt.thingName
			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ── clientToken validation ────────────────────────────────────────────────────

func TestRefinement4_ValidateClientToken_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
	}{
		{name: "empty_token", token: ""},
		{name: "short_token", token: "abc"},
		{name: "alphanumeric", token: "req-12345-abc"},
		{name: "max_length", token: strings.Repeat("a", iotdataplane.MaxClientTokenLength)},
		{name: "special_chars", token: "token_123-ABC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateClientToken(tt.token)
			assert.NoError(t, err, "token %q should be valid", tt.token)
		})
	}
}

func TestRefinement4_ValidateClientToken_TooLong(t *testing.T) {
	t.Parallel()

	overlong := strings.Repeat("x", iotdataplane.MaxClientTokenLength+1)
	err := iotdataplane.ValidateClientToken(overlong)
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

func TestRefinement4_ClientToken_TooLong_ViaHTTP(t *testing.T) {
	t.Parallel()

	overlong := strings.Repeat("x", iotdataplane.MaxClientTokenLength+1)
	body := fmt.Sprintf(`{"state":{"desired":{"k":"v"}},"clientToken":%q}`, overlong)

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/things/device1/shadow", []byte(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
}

func TestRefinement4_ClientToken_Exactly64Chars_Accepted(t *testing.T) {
	t.Parallel()

	exact := strings.Repeat("a", iotdataplane.MaxClientTokenLength)
	body := fmt.Sprintf(`{"state":{"desired":{"k":"v"}},"clientToken":%q}`, exact)

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/things/device1/shadow", []byte(body))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, exact, resp["clientToken"])
}

// ── Shadow state key required ─────────────────────────────────────────────────

func TestRefinement4_ShadowUpdate_StateRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing_state_key",
			body:     `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "state_is_null",
			body:     `{"state":null}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "state_is_array",
			body:     `{"state":[1,2,3]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "version_only_no_state",
			body:     `{"version":0}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "clienttoken_only_no_state",
			body:     `{"clientToken":"tok"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_state_section_ok",
			body:     `{"state":{}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "state_with_desired_ok",
			body:     `{"state":{"desired":{"k":"v"}}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "state_with_reported_ok",
			body:     `{"state":{"reported":{"temp":72}}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "state_with_both_ok",
			body:     `{"state":{"desired":{"k":"v"},"reported":{"k":"v"}}}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/things/device1/shadow", []byte(tt.body))
			assert.Equal(t, tt.wantCode, rec.Code, "body=%q", tt.body)
		})
	}
}

func TestRefinement4_ShadowUpdate_StateRequired_ErrorShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/things/device1/shadow", []byte(`{}`))

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
	assert.Contains(t, resp["message"], "state")
}

// ── desired: null and reported: null wipe behavior ───────────────────────────

func TestRefinement4_ShadowDesiredNull_WipesDesired(t *testing.T) {
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

func TestRefinement4_ShadowDesiredNull_LeavesReportedIntact(t *testing.T) {
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

func TestRefinement4_ShadowReportedNull_WipesReported(t *testing.T) {
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

func TestRefinement4_ShadowBothNull_WipesBoth(t *testing.T) {
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

func TestRefinement4_ShadowDesiredNull_ThenResetDesired(t *testing.T) {
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

func TestRefinement4_ShadowDesiredNull_VersionStillIncrements(t *testing.T) {
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

func TestRefinement4_ShadowDesiredNull_ViaHTTP(t *testing.T) {
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

// ── Shadow response: empty sections omitted ───────────────────────────────────

func TestRefinement4_ShadowResponse_OnlyDesiredSet_ReportedOmitted(t *testing.T) {
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

func TestRefinement4_ShadowResponse_OnlyReportedSet_DesiredOmitted(t *testing.T) {
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

func TestRefinement4_ShadowResponse_BothSet_BothPresent(t *testing.T) {
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

func TestRefinement4_ShadowResponse_EmptyStateSection_StateObjectPresent(t *testing.T) {
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

// ── Shadow desired/reported section must be an object ─────────────────────────

func TestRefinement4_ShadowUpdate_DesiredMustBeObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired string
	}{
		{name: "array", desired: `["a","b"]`},
		{name: "string", desired: `"hello"`},
		{name: "number", desired: `42`},
		{name: "bool_true", desired: `true`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := fmt.Sprintf(`{"state":{"desired":%s}}`, tt.desired)
			b := iotdataplane.NewInMemoryBackend()
			_, err := b.UpdateThingShadow("dev", "", []byte(body))
			require.ErrorIs(t, err, iotdataplane.ErrValidation, "desired=%s must be rejected", tt.desired)
		})
	}
}

func TestRefinement4_ShadowUpdate_ReportedMustBeObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reported string
	}{
		{name: "array", reported: `[1,2,3]`},
		{name: "string", reported: `"sensor"`},
		{name: "number", reported: `99`},
		{name: "bool_false", reported: `false`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := fmt.Sprintf(`{"state":{"reported":%s}}`, tt.reported)
			b := iotdataplane.NewInMemoryBackend()
			_, err := b.UpdateThingShadow("dev", "", []byte(body))
			require.ErrorIs(t, err, iotdataplane.ErrValidation, "reported=%s must be rejected", tt.reported)
		})
	}
}

// ── Shadow metadata cleared on null wipe ─────────────────────────────────────

func TestRefinement4_ShadowMetadata_ClearedOnDesiredNull(t *testing.T) {
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

// ── Shadow delta when one section cleared ────────────────────────────────────

func TestRefinement4_ShadowDelta_AfterDesiredWipe_NoDelta(t *testing.T) {
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

// ── Cross-thing and cross-shadow isolation ────────────────────────────────────

func TestRefinement4_CrossThing_ShadowIsolation(t *testing.T) {
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

func TestRefinement4_ClassicVsNamedShadow_Isolation(t *testing.T) {
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

func TestRefinement4_NamedShadow_IndependentVersions(t *testing.T) {
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

// ── Shadow update idempotency and merge correctness ──────────────────────────

func TestRefinement4_ShadowUpdate_EmptyDesiredPatch_NoOpForKeys(t *testing.T) {
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

func TestRefinement4_ShadowUpdate_MultiplePatchesAccumulate(t *testing.T) {
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

// ── Shadow optimistic locking with state required ─────────────────────────────

func TestRefinement4_ShadowVersionLock_WithStateRequired(t *testing.T) {
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

// ── Shadow shadow cap enforcement with correct documents ──────────────────────

func TestRefinement4_ShadowCap_CapEnforcedWithValidDocs(t *testing.T) {
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

func TestRefinement4_ShadowCap_UpdateExistingAlwaysSucceeds(t *testing.T) {
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

// ── Shadow backend: GetThingShadow thingName validation ──────────────────────

func TestRefinement4_Backend_GetThingShadow_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "empty", thingName: ""},
		{name: "with_space", thingName: "bad name"},
		{name: "too_long", thingName: strings.Repeat("x", iotdataplane.MaxThingNameLength+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.GetThingShadow(tt.thingName, "")
			require.ErrorIs(t, err, iotdataplane.ErrValidation)
		})
	}
}

func TestRefinement4_Backend_UpdateThingShadow_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "empty", thingName: ""},
		{name: "with_space", thingName: "bad name"},
		{name: "slash", thingName: "a/b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.UpdateThingShadow(tt.thingName, "", []byte(`{"state":{"desired":{"k":"v"}}}`))
			require.ErrorIs(t, err, iotdataplane.ErrValidation)
		})
	}
}

func TestRefinement4_Backend_DeleteThingShadow_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.DeleteThingShadow("bad!name", "")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

func TestRefinement4_Backend_ListNamedShadows_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.ListNamedShadowsForThing("bad name with spaces")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

// ── Publish: various content types and retain interaction ────────────────────

func TestRefinement4_Publish_BinaryContentType_NoUnwrap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        []byte
		retain      string
		wantPayload []byte
	}{
		{
			name:        "binary_payload_preserved",
			contentType: "application/octet-stream",
			body:        []byte{0x00, 0x01, 0xFF, 0xFE},
			retain:      "true",
			wantPayload: []byte{0x00, 0x01, 0xFF, 0xFE},
		},
		{
			name:        "json_payload_unwrapped",
			contentType: "application/json",
			body:        []byte(`{"payload":"hello"}`),
			retain:      "true",
			wantPayload: []byte("hello"),
		},
		{
			name:        "json_payload_no_envelope_preserved",
			contentType: "application/json",
			body:        []byte(`{"key":"value"}`),
			retain:      "true",
			wantPayload: []byte(`{"key":"value"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			h := iotdataplane.NewHandler(b)

			rec := doRequestWithContentType(t, h, http.MethodPost,
				"/topics/test/topic?retain="+tt.retain, tt.contentType, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			msg, err := b.GetRetainedMessage("test/topic")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPayload, msg.Payload)
		})
	}
}

// ── Retained message: cap and error shapes ────────────────────────────────────

func TestRefinement4_RetainedMessage_GetNonexistent_404Shape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage/no/such/topic", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["error"])
}

func TestRefinement4_RetainedMessage_Lifecycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	require.NoError(t, b.StoreRetainedMessage("home/temp", []byte("72"), 0))
	require.NoError(t, b.StoreRetainedMessage("home/humidity", []byte("45"), 1))
	require.NoError(t, b.StoreRetainedMessage("home/co2", []byte("400"), 0))

	assert.Equal(t, 3, iotdataplane.RetainedMessageCount(b))

	// Get one.
	msg, err := b.GetRetainedMessage("home/temp")
	require.NoError(t, err)
	assert.Equal(t, "home/temp", msg.Topic)
	assert.Equal(t, []byte("72"), msg.Payload)

	// Empty payload removes.
	require.NoError(t, b.StoreRetainedMessage("home/temp", []byte{}, 0))
	assert.Equal(t, 2, iotdataplane.RetainedMessageCount(b))

	_, err = b.GetRetainedMessage("home/temp")
	require.ErrorIs(t, err, iotdataplane.ErrRetainedMessageNotFound)

	// List is sorted by topic.
	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "home/co2", msgs[0].Topic)
	assert.Equal(t, "home/humidity", msgs[1].Topic)
}

func TestRefinement4_RetainedMessage_LastModifiedTime_IsMillis(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/data", []byte("val"), 0))

	msg, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)

	// lastModifiedTime is epoch milliseconds: must be > 1e12 (year 2001+).
	assert.Greater(t, msg.LastModifiedTime, int64(1e12), "lastModifiedTime must be epoch milliseconds")
}

// ── ListRetainedMessages: pagination edge cases ───────────────────────────────

func TestRefinement4_ListRetainedMessages_MaxResultsAlias(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 10 {
		topic := fmt.Sprintf("sensor/%02d/data", i)
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("v"), 0))
	}

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics := resp["retainedTopics"].([]any)
	assert.Len(t, topics, 3)
	_, hasNext := resp["nextToken"]
	assert.True(t, hasNext, "nextToken must be present when more pages exist")
}

func TestRefinement4_ListRetainedMessages_SummaryNoQos(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("a/b", []byte("payload"), 1))

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	topics := resp["retainedTopics"].([]any)
	require.Len(t, topics, 1)

	summary := topics[0].(map[string]any)
	assert.Contains(t, summary, "topic")
	assert.Contains(t, summary, "payloadSize")
	assert.Contains(t, summary, "lastModifiedTime")
	// qos must NOT appear in RetainedMessageSummary per AWS spec.
	_, hasQos := summary["qos"]
	assert.False(t, hasQos, "qos must not appear in list summary")
}

// ── ListThingsWithShadows: pagination and isolation ───────────────────────────

func TestRefinement4_ListThingsWithShadows_IncludesTimestamp(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing-x", "", []byte(`{"state":{"desired":{"k":"v"}}}`))

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "things")
	assert.Contains(t, resp, "timestamp")

	ts := resp["timestamp"].(float64)
	assert.Greater(t, ts, float64(1e9), "timestamp must be epoch seconds > 1e9")
}

func TestRefinement4_ListThingsWithShadows_Pagination(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 5 {
		name := fmt.Sprintf("thing-%02d", i)
		b.AddShadowInternal(name, "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	}

	h := iotdataplane.NewHandler(b)

	// First page: 2 items.
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows?pageSize=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	things1 := page1["things"].([]any)
	assert.Len(t, things1, 2)
	nextToken := page1["nextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page using token.
	paginatedURL := "/api/things/shadow/ListThingsWithShadows?pageSize=2&nextToken=" + nextToken
	rec2 := doRequest(t, h, http.MethodGet, paginatedURL, nil)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	things2 := page2["things"].([]any)
	assert.Len(t, things2, 2)

	// Collect all and verify no duplicates.
	seen := map[string]bool{}
	for _, item := range append(things1, things2...) {
		key := item.(string)
		assert.False(t, seen[key], "duplicate thing %q across pages", key)
		seen[key] = true
	}
}

// ── Connections: error shapes and validation ──────────────────────────────────

func TestRefinement4_Connection_Register_DuplicateShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceAlreadyExistsException", resp["error"])
}

func TestRefinement4_Connection_EmptyClientID_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// POST /_admin/connections/ with empty clientId segment.
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement4_Connection_DollarPrefix_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system-client", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
}

func TestRefinement4_Connection_DeleteIdempotent_DollarRejected(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Deleting a nonexistent ID is idempotent.
	err := b.DeleteConnection("nonexistent-client")
	require.NoError(t, err, "delete of nonexistent client must be idempotent")

	// Dollar-prefix rejected even for delete.
	err = b.DeleteConnection("$system")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

// ── Error response shape coverage ────────────────────────────────────────────

func TestRefinement4_ErrorShapes_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		wantError string
		body      []byte
		wantCode  int
	}{
		{
			name:      "shadow_not_found",
			method:    http.MethodGet,
			path:      "/things/missing-thing/shadow",
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "retained_message_not_found",
			method:    http.MethodGet,
			path:      "/retainedMessage/no/such/topic",
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "invalid_request_bad_state",
			method:    http.MethodPost,
			path:      "/things/device1/shadow",
			body:      []byte(`{}`),
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidRequestException",
		},
		{
			name:      "invalid_request_wildcard_topic",
			method:    http.MethodPost,
			path:      "/topics/bad/+/wildcard",
			body:      []byte(`{"data":"val"}`),
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidRequestException",
		},
		{
			name:      "duplicate_connection",
			method:    http.MethodPost,
			path:      "/_admin/connections/dup-client",
			wantCode:  http.StatusConflict,
			wantError: "ResourceAlreadyExistsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

			// Pre-seed for duplicate connection test.
			if tt.name == "duplicate_connection" {
				doRequest(t, h, http.MethodPost, "/_admin/connections/dup-client", nil)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "unexpected status for %s %s", tt.method, tt.path)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantError, resp["error"],
				"unexpected error type for %s %s", tt.method, tt.path)
			_, hasMsg := resp["message"]
			assert.True(t, hasMsg, "error response must include message field")
		})
	}
}

func TestRefinement4_VersionConflict_ErrorShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev/shadow", []byte(`{"state":{"desired":{"k":"v"}}}`))

	rec := doRequest(t, h, http.MethodPost, "/things/dev/shadow",
		[]byte(`{"state":{"desired":{"k":"v2"}},"version":99}`))
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "VersionConflictException", resp["error"])
	// AWS includes the numeric code in the body.
	code, hasCode := resp["code"]
	require.True(t, hasCode, "VersionConflictException body must include code")
	assert.InDelta(t, float64(http.StatusConflict), code, 0)
}

// ── Persistence: snapshot/restore with new behaviors ─────────────────────────

func TestRefinement4_Persistence_NullWipeSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := iotdataplane.NewInMemoryBackend()

	_, err := b1.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"},"reported":{"temp":72}}}`))
	require.NoError(t, err)
	// Wipe desired.
	_, err = b1.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	resp, err := b2.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))
	state := doc["state"].(map[string]any)

	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must still be absent after restore")

	reported, hasReported := state["reported"].(map[string]any)
	require.True(t, hasReported, "reported must survive restore")
	assert.InDelta(t, float64(72), reported["temp"], 0)
}

// ── Topic validation edge cases ───────────────────────────────────────────────

func TestRefinement4_TopicValidation_Matrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "simple", topic: "home/sensors/temperature", wantErr: false},
		{name: "single_segment", topic: "temperature", wantErr: false},
		{name: "dollar_not_shadow", topic: "$aws/jobs/start", wantErr: false},
		{name: "shadow_reserved", topic: "$aws/things/dev/shadow/update", wantErr: true},
		{name: "wildcard_hash", topic: "home/#", wantErr: true},
		{name: "wildcard_plus", topic: "home/+/temp", wantErr: true},
		{name: "empty_level_leading", topic: "/home/temp", wantErr: true},
		{name: "empty_level_trailing", topic: "home/temp/", wantErr: true},
		{name: "empty_level_middle", topic: "home//temp", wantErr: true},
		{name: "too_long", topic: strings.Repeat("a", 257), wantErr: true},
		{name: "exactly_256", topic: strings.Repeat("a", 256), wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.ErrorIs(t, err, iotdataplane.ErrValidation, "topic %q should be invalid", tt.topic)
			} else {
				assert.NoError(t, err, "topic %q should be valid", tt.topic)
			}
		})
	}
}

// ── Helper functions used by this test file ───────────────────────────────────

// doRequestWithContentType issues a handler request with a specific Content-Type header.
func doRequestWithContentType(
	t *testing.T, h *iotdataplane.Handler, method, path, contentType string, body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}
