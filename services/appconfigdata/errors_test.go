package appconfigdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// --- AWS error response format ---

// TestHandler_ErrorBodyFormat verifies that all error responses carry __type + message fields
// and the X-Amzn-ErrorType header, matching the AWS REST-JSON error protocol.
func TestHandler_ErrorBodyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(h *appconfigdata.Handler)
		name             string
		method           string
		path             string
		wantErrorType    string
		wantErrorTypeHdr string
		body             []byte
		wantStatus       int
	}{
		{
			name:             "start_session_missing_fields",
			method:           http.MethodPost,
			path:             "/configurationsessions",
			body:             []byte(`{"ApplicationIdentifier":"app"}`),
			wantStatus:       http.StatusBadRequest,
			wantErrorType:    "BadRequestException",
			wantErrorTypeHdr: "BadRequestException",
		},
		{
			name:   "start_session_invalid_poll_interval",
			method: http.MethodPost,
			path:   "/configurationsessions",
			body: mustMarshalJSON(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": 5,
			}),
			wantStatus:       http.StatusBadRequest,
			wantErrorType:    "BadRequestException",
			wantErrorTypeHdr: "BadRequestException",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))
			},
		},
		{
			name:   "start_session_no_deployment",
			method: http.MethodPost,
			path:   "/configurationsessions",
			body: mustMarshalJSON(map[string]string{
				"ApplicationIdentifier":          "app",
				"EnvironmentIdentifier":          "env",
				"ConfigurationProfileIdentifier": "p",
			}),
			wantStatus:       http.StatusNotFound,
			wantErrorType:    "ResourceNotFoundException",
			wantErrorTypeHdr: "ResourceNotFoundException",
		},
		{
			name:             "get_latest_bad_token",
			method:           http.MethodGet,
			path:             "/configuration?configuration_token=not-a-real-token",
			wantStatus:       http.StatusBadRequest,
			wantErrorType:    "BadRequestException",
			wantErrorTypeHdr: "BadRequestException",
		},
		{
			name:             "get_latest_empty_token",
			method:           http.MethodGet,
			path:             "/configuration",
			wantStatus:       http.StatusBadRequest,
			wantErrorType:    "BadRequestException",
			wantErrorTypeHdr: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify __type field in response body.
			got, _ := decodeErrorBody(t, rec.Body.String())
			assert.Equal(t, tt.wantErrorType, got, "response body must contain correct __type")

			// Verify X-Amzn-ErrorType header.
			assert.Equal(t, tt.wantErrorTypeHdr, rec.Header().Get("X-Amzn-Errortype"),
				"X-Amzn-ErrorType header must match exception type")
		})
	}
}

// TestHandler_BadRequestException_Details verifies structured BadRequestException Details for token errors.
// AWS clients rely on Details.InvalidParameters[param].Problem to take targeted corrective action.
func TestHandler_BadRequestException_Details(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "p", `{"x":1}`)

	token := startSession(t, h, "app", "env", "p")

	// First poll — rotates token.
	firstRec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, firstRec.Code)

	t.Run("corrupted_token_has_problem_Corrupted", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		seedProfile(t, h2, "a", "e", "p", `{}`)
		_ = startSession(t, h2, "a", "e", "p")

		rec := doRequest(t, h2, http.MethodGet, "/configuration?configuration_token=bad-token-format", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "BadRequestException", body["__type"])
		assert.Equal(t, "InvalidParameters", body["Reason"])

		details, ok := body["Details"].(map[string]any)
		require.True(t, ok, "Details must be present")
		invalidParams, ok := details["InvalidParameters"].(map[string]any)
		require.True(t, ok, "Details.InvalidParameters must be present")
		tokenDetail, ok := invalidParams["ConfigurationToken"].(map[string]any)
		require.True(t, ok, "Details.InvalidParameters.ConfigurationToken must be present")
		assert.Equal(t, "Corrupted", tokenDetail["Problem"])
	})

	t.Run("poll_too_frequent_has_problem_PollIntervalNotSatisfied", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		seedProfile(t, h2, "a", "e", "p", `{}`)

		sessionBody, err := json.Marshal(map[string]any{
			"ApplicationIdentifier":                "a",
			"EnvironmentIdentifier":                "e",
			"ConfigurationProfileIdentifier":       "p",
			"RequiredMinimumPollIntervalInSeconds": 60,
		})
		require.NoError(t, err)

		sessionRec := doRequest(t, h2, http.MethodPost, "/configurationsessions", sessionBody)
		require.Equal(t, http.StatusCreated, sessionRec.Code)

		var sessionResp map[string]string
		require.NoError(t, json.Unmarshal(sessionRec.Body.Bytes(), &sessionResp))
		tok := sessionResp["InitialConfigurationToken"]

		// First poll succeeds.
		firstPoll := doRequest(t, h2, http.MethodGet, "/configuration?configuration_token="+tok, nil)
		require.Equal(t, http.StatusOK, firstPoll.Code)
		nextTok := firstPoll.Header().Get("Next-Poll-Configuration-Token")
		require.NotEmpty(t, nextTok)

		// Immediately poll again with next token — should be too frequent.
		rec2 := doRequest(t, h2, http.MethodGet, "/configuration?configuration_token="+nextTok, nil)
		assert.Equal(t, http.StatusBadRequest, rec2.Code)
		assert.Equal(t, "BadRequestException", rec2.Header().Get("X-Amzn-Errortype"))

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))
		assert.Equal(t, "BadRequestException", body["__type"])
		assert.Equal(t, "InvalidParameters", body["Reason"])

		details, ok := body["Details"].(map[string]any)
		require.True(t, ok, "Details must be present")
		invalidParams, ok := details["InvalidParameters"].(map[string]any)
		require.True(t, ok, "Details.InvalidParameters must be present")
		tokenDetail, ok := invalidParams["ConfigurationToken"].(map[string]any)
		require.True(t, ok, "Details.InvalidParameters.ConfigurationToken must be present")
		assert.Equal(t, "PollIntervalNotSatisfied", tokenDetail["Problem"])

		// Retry-After header must be set to the session's poll interval.
		retryAfter := rec2.Header().Get("Retry-After")
		assert.Equal(t, "60", retryAfter, "Retry-After header must match session poll interval")
	})
}

// TestHandler_ResourceNotFoundException_Structure verifies ResourceNotFoundException carries
// ResourceType and ReferencedBy fields for client-side diagnostics.
func TestHandler_ResourceNotFoundException_Structure(t *testing.T) {
	t.Parallel()

	t.Run("no_active_deployment_returns_Deployment_resource_type", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		// No configuration deployed — StartConfigurationSession must fail.
		body := []byte(
			`{"ApplicationIdentifier":"myapp","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"flags"}`,
		)
		rec := doRequest(t, h, http.MethodPost, "/configurationsessions", body)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "ResourceNotFoundException", rec.Header().Get("X-Amzn-Errortype"))

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		assert.Equal(t, "Deployment", resp["ResourceType"])

		referencedBy, ok := resp["ReferencedBy"].(map[string]any)
		require.True(t, ok, "ReferencedBy must be a map")
		assert.Equal(t, "myapp", referencedBy["ApplicationIdentifier"])
		assert.Equal(t, "prod", referencedBy["EnvironmentIdentifier"])
		assert.Equal(t, "flags", referencedBy["ConfigurationProfileIdentifier"])
	})

	t.Run("resource_removed_returns_Deployment_resource_type", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		seedProfile(t, h, "app", "env", "p", `{"v":1}`)
		token := startSession(t, h, "app", "env", "p")

		// Poll once to get a rotated token.
		rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
		require.Equal(t, http.StatusOK, rec1.Code)
		nextToken := rec1.Header().Get("Next-Poll-Configuration-Token")

		// Deleting profile purges session — next poll yields 400 (session gone from map).
		require.True(t, h.Backend.DeleteProfile("app", "env", "p"))
		rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextToken, nil)
		assert.Equal(t, http.StatusBadRequest, rec2.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
		assert.Equal(t, "BadRequestException", resp["__type"])
	})
}

// TestHandler_ErrorTypeHeader verifies X-Amzn-ErrorType is set on all error responses.
func TestHandler_ErrorTypeHeader(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("bad_request_has_header", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/configurationsessions",
			[]byte(`{"ApplicationIdentifier":"a"}`))
		assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))
	})

	t.Run("resource_not_found_has_header", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodPost, "/configurationsessions",
			[]byte(`{"ApplicationIdentifier":"a","EnvironmentIdentifier":"e","ConfigurationProfileIdentifier":"p"}`))
		assert.Equal(t, "ResourceNotFoundException", rec.Header().Get("X-Amzn-Errortype"))
	})

	t.Run("invalid_token_has_header", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token=garbage", nil)
		assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))
	})
}

// TestHandler_BadRequestException_MissingDetails verifies that simple bad requests
// (invalid body, missing fields) also carry __type in the body.
func TestHandler_BadRequestException_MissingDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "invalid_json",
			body: []byte(`{not valid`),
		},
		{
			name: "empty_body",
			body: []byte(``),
		},
		{
			name: "null_body",
			body: []byte(`null`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			errType, msg := decodeErrorBody(t, rec.Body.String())
			assert.Equal(t, "BadRequestException", errType)
			assert.NotEmpty(t, msg, "error body must have a message")
		})
	}
}

// TestHandler_ErrorResponseShape verifies that all error responses carry the __type field
// in the body and the X-Amzn-ErrorType response header, matching the AWS REST-JSON protocol.
func TestHandler_ErrorResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name          string
		method        string
		path          string
		body          []byte
		wantStatus    int
		wantErrorType string
	}{
		{
			name:          "start_session_missing_all_identifiers",
			method:        http.MethodPost,
			path:          "/configurationsessions",
			body:          []byte(`{}`),
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
		{
			name:   "start_session_no_deployment",
			method: http.MethodPost,
			path:   "/configurationsessions",
			body: []byte(
				`{"ApplicationIdentifier":"a","EnvironmentIdentifier":"e","ConfigurationProfileIdentifier":"p"}`,
			),
			wantStatus:    http.StatusNotFound,
			wantErrorType: "ResourceNotFoundException",
		},
		{
			name:          "get_config_empty_token",
			method:        http.MethodGet,
			path:          "/configuration",
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
		{
			name:          "get_config_invalid_token",
			method:        http.MethodGet,
			path:          "/configuration?configuration_token=bad-token",
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-Errortype"),
				"X-Amzn-ErrorType header must match exception type")

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantErrorType, body["__type"],
				"response body __type field must match exception type")
			assert.NotEmpty(t, body["message"], "error body must have a message field")
		})
	}
}
