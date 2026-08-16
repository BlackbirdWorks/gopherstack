package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func TestBackend_ShadowUpdate_NonObjectDocumentRejected(t *testing.T) {
	t.Parallel()

	// AWS IoT requires shadow documents to be JSON objects. Arrays, primitives,
	// and plain strings must be rejected with ErrValidation.
	b := iotdataplane.NewInMemoryBackend()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "json_array", payload: []byte(`["a","b","c"]`)},
		{name: "plain_string", payload: []byte(`"just a string"`)},
		{name: "number", payload: []byte(`42`)},
		{name: "empty", payload: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.UpdateThingShadow("thing-"+tt.name, "", tt.payload)
			require.ErrorIs(t, err, iotdataplane.ErrValidation, "non-object document must be rejected")
		})
	}
}
func TestBackend_ShadowUpdate_ObjectDocumentSucceeds(t *testing.T) {
	t.Parallel()

	// Valid JSON object documents must be accepted and the response must include
	// version + timestamp (verifies the response is built before state mutation).
	b := iotdataplane.NewInMemoryBackend()

	resp, err := b.UpdateThingShadow("thing1", "", []byte(`{"state":{"desired":{"key":"value"}}}`))
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp, &result), "response must be valid JSON")
	assert.Contains(t, result, "version", "response must contain version")
	assert.Contains(t, result, "timestamp", "response must contain timestamp")
}

// TestParityAccuracy_UpdateThingShadow_StateRequired verifies that UpdateThingShadow
// rejects requests without a `state` field, matching real AWS InvalidRequestException.
func Test_UpdateThingShadow_StateRequired(t *testing.T) {
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

// TestParityAccuracy_VersionConflict_RejectedWithCorrectError verifies that
// UpdateThingShadow rejects version mismatches with ConflictException (the
// real AWS iotdataplane exception name; there is no "VersionConflictException"
// in the real API -- see aws-sdk-go-v2/service/iotdataplane/types.ConflictException),
// matching real AWS IoT optimistic locking behavior.
func Test_VersionConflict_RejectedWithCorrectError(t *testing.T) {
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
			wantErrorType: "ConflictException",
		},
		{
			name:          "future_version_rejected",
			versionInDoc:  99,
			wantStatus:    http.StatusConflict,
			wantErrorType: "ConflictException",
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
					"error type must be ConflictException")
			}
		})
	}
}
func Test_ShadowNameValidation_InvalidChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryParam string // URL-encoded query string
		wantCode   int
	}{
		{name: "valid_alphanumeric", queryParam: "myShadow123", wantCode: http.StatusNotFound},
		{name: "valid_with_colon", queryParam: "ns:shadow", wantCode: http.StatusNotFound},
		{name: "valid_with_underscore", queryParam: "my_shadow", wantCode: http.StatusNotFound},
		{name: "valid_with_hyphen", queryParam: "my-shadow", wantCode: http.StatusNotFound},
		{name: "invalid_space", queryParam: "my%20shadow", wantCode: http.StatusBadRequest},
		{name: "invalid_dot", queryParam: "my.shadow", wantCode: http.StatusBadRequest},
		{name: "invalid_at", queryParam: "my%40shadow", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+tt.queryParam, nil)
			assert.Equal(t, tt.wantCode, rec.Code, "queryParam: %q", tt.queryParam)
		})
	}
}
func Test_ShadowNameValidation_TooLong(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// 65-character shadow name must be rejected.
	longName := strings.Repeat("a", 65)
	rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+longName, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// 64-character shadow name must be accepted (name valid, just thing not found).
	exactName := strings.Repeat("a", 64)
	rec = doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+exactName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
func Test_ShadowDocumentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{name: "valid_object", body: []byte(`{"state":{"desired":{}}}`), wantCode: http.StatusOK},
		// AWS requires "state" key; {} without it returns 400 InvalidRequestException.
		{name: "empty_object", body: []byte(`{}`), wantCode: http.StatusBadRequest},
		{name: "array", body: []byte(`["a"]`), wantCode: http.StatusBadRequest},
		{name: "number", body: []byte(`42`), wantCode: http.StatusBadRequest},
		{name: "string", body: []byte(`"hello"`), wantCode: http.StatusBadRequest},
		{name: "empty", body: []byte{}, wantCode: http.StatusBadRequest},
		{name: "null", body: []byte(`null`), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/things/thing1/shadow", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %q", string(tt.body))
		})
	}
}
func Test_ShadowDocumentValidation_TooLarge(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Build a document just over the HTTP body limit (maxShadowBodyBytes = 8KB).
	// The HTTP MaxBytesReader fires before backend validation, so expect 413.
	value := strings.Repeat("v", iotdataplane.MaxShadowDocumentBytes+1)
	oversize := fmt.Sprintf(`{"k":%q}`, value)

	rec := doRequest(t, h, http.MethodPost, "/things/thing1/shadow", []byte(oversize))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}
func Test_ShadowDocumentValidation_BackendSizeCheck(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// The backend-level size check catches documents that exceed MaxShadowDocumentBytes
	// (used when the backend is invoked directly, bypassing HTTP body limits).
	value := strings.Repeat("v", iotdataplane.MaxShadowDocumentBytes+1)
	oversize := fmt.Appendf(nil, `{"k":%q}`, value)

	_, err := b.UpdateThingShadow("thing1", "", oversize)
	require.ErrorIs(t, err, iotdataplane.ErrRequestTooLarge,
		"oversized shadow doc must be RequestEntityTooLargeException, not InvalidRequestException")
}

// Test_ShadowStateDepth_AtMaxAccepted, Test_ShadowStateDepth_ExceedsMaxRejected,
// and Test_ShadowStateDepth_ViaHTTP live in whitebox_test.go: they need direct
// access to the unexported maxShadowStateDepth constant.
func Test_ShadowName_ReservedKeywords(t *testing.T) {
	t.Parallel()

	reserved := []string{
		"update", "get", "delete", "accepted", "rejected", "delta", "documents",
	}

	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+name, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "reserved name %q should be rejected", name)
			assert.Contains(t, rec.Body.String(), "InvalidRequestException")
		})
	}
}
func Test_ShadowName_NonReservedAllowed(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "myConfig", []byte(`{"state":{"desired":{"k":"v"}}}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name=myConfig", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
func Test_ShadowVersionConflict_BodyHasCode(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-vc/shadow", []byte(`{"state":{"desired":{"k":"v"}}}`))

	rec := doRequest(t, h, http.MethodPost, "/things/dev-vc/shadow", []byte(`{
		"state": {"desired": {"k": "v2"}},
		"version": 0
	}`))
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	code, hasCode := resp["code"]
	require.True(t, hasCode, "409 body must include 'code' field")
	assert.InDelta(t, float64(http.StatusConflict), code, 0)
}
func Test_ValidateThingName_ValidNames(t *testing.T) {
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
func Test_ValidateThingName_InvalidNames(t *testing.T) {
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
func Test_ThingName_ValidationViaHTTP(t *testing.T) {
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
func Test_ThingName_ValidationOnGet(t *testing.T) {
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
func Test_ThingName_ValidationOnDelete(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	// URL-encode the space so httptest.NewRequest doesn't panic on an invalid URL.
	rec := doRequest(t, h, http.MethodDelete, "/things/bad%20name/shadow", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "error")
}
func Test_ThingName_ValidationOnListNamedShadows(t *testing.T) {
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
func Test_ValidateClientToken_Valid(t *testing.T) {
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
func Test_ValidateClientToken_TooLong(t *testing.T) {
	t.Parallel()

	overlong := strings.Repeat("x", iotdataplane.MaxClientTokenLength+1)
	err := iotdataplane.ValidateClientToken(overlong)
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
func Test_ClientToken_TooLong_ViaHTTP(t *testing.T) {
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
func Test_ClientToken_Exactly64Chars_Accepted(t *testing.T) {
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
func Test_ShadowUpdate_StateRequired(t *testing.T) {
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
func Test_ShadowUpdate_StateRequired_ErrorShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/things/device1/shadow", []byte(`{}`))

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
	assert.Contains(t, resp["message"], "state")
}
func Test_ShadowUpdate_DesiredMustBeObject(t *testing.T) {
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
func Test_ShadowUpdate_ReportedMustBeObject(t *testing.T) {
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
func Test_GetThingShadow_InvalidThingName(t *testing.T) {
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
func Test_UpdateThingShadow_InvalidThingName(t *testing.T) {
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
func Test_DeleteThingShadow_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.DeleteThingShadow("bad!name", "")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
func Test_ListNamedShadows_InvalidThingName(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.ListNamedShadowsForThing("bad name with spaces")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}
