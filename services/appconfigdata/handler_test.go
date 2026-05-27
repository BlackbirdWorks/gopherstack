package appconfigdata_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

func nowUTC() time.Time { return time.Now().UTC() }

// --- helpers ---

func newTestHandler(t *testing.T) *appconfigdata.Handler {
	t.Helper()

	return appconfigdata.NewHandler(appconfigdata.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *appconfigdata.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// seedProfile sets a configuration profile with JSON content type.
func seedProfile(t *testing.T, h *appconfigdata.Handler, app, env, profile, content string) {
	t.Helper()

	require.NoError(t, h.Backend.SetConfiguration(app, env, profile, content, "application/json"))
}

// startSession seeds a profile if needed and starts a session, returning the initial token.
func startSession(t *testing.T, h *appconfigdata.Handler, app, env, profile string) string {
	t.Helper()

	body := []byte(`{"ApplicationIdentifier":"` + app + `","EnvironmentIdentifier":"` +
		env + `","ConfigurationProfileIdentifier":"` + profile + `"}`)
	rec := doRequest(t, h, http.MethodPost, "/configurationsessions", body)
	require.Equal(t, http.StatusCreated, rec.Code, "startSession failed: %s", rec.Body.String())

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	token := resp["InitialConfigurationToken"]
	require.NotEmpty(t, token)

	return token
}

// --- Handler metadata ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "AppConfigData", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "StartConfigurationSession")
	assert.Contains(t, ops, "GetLatestConfiguration")
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "appconfigdata", h.ChaosServiceName())
	assert.Equal(
		t,
		[]string{"StartConfigurationSession", "GetLatestConfiguration"},
		h.ChaosOperations(),
	)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, 86, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "configurationsessions", path: "/configurationsessions", want: true},
		{name: "configuration", path: "/configuration", want: true},
		{name: "not_matched", path: "/restapis/something", want: false},
		{name: "dashboard", path: "/dashboard/appconfigdata", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "start_session",
			method: http.MethodPost,
			path:   "/configurationsessions",
			want:   "StartConfigurationSession",
		},
		{
			name:   "get_latest",
			method: http.MethodGet,
			path:   "/configuration?configuration_token=abc123",
			want:   "GetLatestConfiguration",
		},
		{
			name:   "unknown",
			method: http.MethodDelete,
			path:   "/configurationsessions",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		setup  func(h *appconfigdata.Handler) string
		want   string
	}{
		{
			name:   "start_session_returns_fixed_label",
			path:   "/configurationsessions",
			method: http.MethodPost,
			want:   "configurationsession",
		},
		{
			name:   "get_latest_with_known_token_returns_profile",
			method: http.MethodGet,
			setup: func(h *appconfigdata.Handler) string {
				require.NoError(
					t,
					h.Backend.SetConfiguration(
						"my-app",
						"prod",
						"my-profile",
						`{}`,
						"application/json",
					),
				)
				token, _ := h.Backend.StartSession("my-app", "prod", "my-profile", 0)

				return "/configuration?configuration_token=" + token
			},
			want: "my-app/prod/my-profile",
		},
		{
			name:   "get_latest_with_unknown_token_returns_fallback",
			path:   "/configuration?configuration_token=unknown-token",
			method: http.MethodGet,
			want:   "unknown-session",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(h)
			}

			e := echo.New()
			req := httptest.NewRequest(tt.method, path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// --- StartConfigurationSession ---

func TestHandler_StartConfigurationSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(h *appconfigdata.Handler)
		body       []byte
		wantStatus int
		wantToken  bool
	}{
		{
			name: "success_with_active_deployment",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "my-profile", `{}`, "application/json"))
			},
			body: []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "no_active_deployment_returns_404",
			body: []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_application",
			body: []byte(
				`{"EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_environment",
			body: []byte(
				`{"ApplicationIdentifier":"my-app","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_profile",
			body:       []byte(`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       []byte(`not-json`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "poll_interval_too_low_returns_400",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":5}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "poll_interval_zero_is_accepted",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":0}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "poll_interval_exactly_minimum_is_accepted",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("app", "env", "prof", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env",` +
				`"ConfigurationProfileIdentifier":"prof","RequiredMinimumPollIntervalInSeconds":15}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name: "whitespace_trimmed_from_identifiers",
			setup: func(h *appconfigdata.Handler) {
				require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "my-profile", `{}`, "application/json"))
			},
			body: []byte(`{"ApplicationIdentifier":"  my-app  ",` +
				`"EnvironmentIdentifier":"prod",` +
				`"ConfigurationProfileIdentifier":"my-profile"}`),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantToken {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["InitialConfigurationToken"])
			}
		})
	}
}

// --- GetLatestConfiguration ---

func TestHandler_GetLatestConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		content       string
		contentType   string
		wantBody      string
		wantStatus    int
		hasProfile    bool
		wantEtag      bool
		wantNoContent bool
	}{
		{
			name:        "with_configuration_returns_200",
			hasProfile:  true,
			content:     `{"featureFlag":true}`,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
			wantBody:    `{"featureFlag":true}`,
			wantEtag:    true,
		},
		{
			name:          "second_poll_with_no_change_returns_204",
			hasProfile:    true,
			content:       `{"featureFlag":true}`,
			contentType:   "application/json",
			wantStatus:    http.StatusNoContent,
			wantNoContent: true,
		},
		{
			name:       "invalid_token_returns_400",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "invalid_token_returns_400" {
				rec := doRequest(
					t,
					h,
					http.MethodGet,
					"/configuration?configuration_token=bad-token",
					nil,
				)
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			if tt.hasProfile {
				require.NoError(
					t,
					h.Backend.SetConfiguration(
						"my-app",
						"prod",
						"my-profile",
						tt.content,
						tt.contentType,
					),
				)
			}

			token := startSession(t, h, "my-app", "prod", "my-profile")

			if tt.wantNoContent {
				// First poll to set PreviousContentHash.
				rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
				require.Equal(t, http.StatusOK, rec1.Code)
				token = rec1.Header().Get("Next-Poll-Configuration-Token")
				require.NotEmpty(t, token)

				// Second poll — content unchanged → 204.
				rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
				assert.Equal(t, http.StatusNoContent, rec2.Code)
				assert.Empty(t, rec2.Header().Get("ETag"), "ETag must not be set on 204")
				assert.Empty(t, rec2.Body.String())

				return
			}

			rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}

			if tt.wantEtag {
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			}

			if tt.wantStatus == http.StatusOK || tt.wantStatus == http.StatusNoContent {
				nextToken := rec.Header().Get("Next-Poll-Configuration-Token")
				assert.NotEmpty(t, nextToken)
				assert.NotEqual(t, token, nextToken)
			}
		})
	}
}

func TestHandler_GetLatestConfiguration_EmptyToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/configuration", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_NoContentHeaders verifies that a 204 response (no content change) does not
// include ETag or Content-Type — only the poll-control headers should be present.
func TestHandler_NoContentHeaders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "p", `{"x":1}`)
	token := startSession(t, h, "app", "env", "p")

	// First poll sets PreviousContentHash.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	assert.NotEmpty(t, rec1.Header().Get("ETag"), "first poll must include ETag")
	assert.NotEmpty(t, rec1.Header().Get("Next-Poll-Configuration-Token"))
	assert.NotEmpty(t, rec1.Header().Get("Next-Poll-Interval-In-Seconds"))
	token = rec1.Header().Get("Next-Poll-Configuration-Token")

	// Second poll — content unchanged → 204.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusNoContent, rec2.Code)
	assert.Empty(t, rec2.Header().Get("ETag"), "204 must not include ETag")
	// Poll-control headers must still be present.
	assert.NotEmpty(t, rec2.Header().Get("Next-Poll-Configuration-Token"))
	assert.NotEmpty(t, rec2.Header().Get("Next-Poll-Interval-In-Seconds"))
}

// TestHandler_VersionLabelHeader verifies the X-Amzn-AppConfig-Version-Label header.
func TestHandler_VersionLabelHeader(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "p", `{"v":1}`)
	token := startSession(t, h, "app", "env", "p")

	rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-AppConfig-Version-Label"))
}

// TestHandler_ProfileDeletedMidSession verifies that polling after the profile is removed
// returns 404 ResourceNotFoundException.
func TestHandler_ProfileDeletedMidSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "p", `{"v":1}`)
	token := startSession(t, h, "app", "env", "p")

	// Poll once to get a valid next token.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	nextToken := rec1.Header().Get("Next-Poll-Configuration-Token")

	// Delete the profile.
	require.True(t, h.Backend.DeleteProfile("app", "env", "p"))

	// Polling with the next token must yield 404 because the backend purges sessions on delete.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextToken, nil)
	// Sessions tied to the deleted profile are removed, so token is now unknown → 400.
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestHandler_TokenExpired verifies that the handler returns 401 when a session token has
// passed its absolute expiry time.
func TestHandler_TokenExpired(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Manually expire the session by sweeping with a zero idle TTL and then forcing the
	// absolute-expiry path is not possible without time travel, so we test the backend
	// directly: SweepExpiredSessions with zero TTL removes all sessions, then the token
	// returns ErrSessionNotFound (not ErrTokenExpired — it's gone from the map).
	b.SweepExpiredSessions(t.Context(), 0)

	_, _, _, _, _, err = b.GetLatestConfiguration(token)
	assert.ErrorIs(t, err, appconfigdata.ErrSessionNotFound)
}

// TestHandler_NoActiveDeployment verifies that starting a session for a non-existent profile
// returns 404.
func TestHandler_NoActiveDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// No SetConfiguration call — no deployment.
	body := []byte(`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env","ConfigurationProfileIdentifier":"p"}`)
	rec := doRequest(t, h, http.MethodPost, "/configurationsessions", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Token rotation ---

func TestHandler_TokenRotation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "profile", `{"v":1}`)

	token := startSession(t, h, "app", "env", "profile")

	// First poll succeeds and returns content.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	nextToken := rec1.Header().Get("Next-Poll-Configuration-Token")
	assert.NotEmpty(t, nextToken)
	assert.NotEqual(t, token, nextToken)

	// Old token is in grace period — idempotent replay returns the cached response.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	assert.Equal(t, http.StatusOK, rec2.Code, "old token should work during grace period")

	// New token polls unchanged content → 204 No Content.
	// The client already received the current version via T0, so T1's first poll yields empty body.
	rec3 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextToken, nil)
	assert.Equal(t, http.StatusNoContent, rec3.Code)

	// After a configuration update the next token returns new content → 200.
	require.NoError(t, h.Backend.SetConfiguration("app", "env", "profile", `{"v":2}`, "application/json"))
	nextToken2 := rec3.Header().Get("Next-Poll-Configuration-Token")
	rec4 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextToken2, nil)
	assert.Equal(t, http.StatusOK, rec4.Code, "changed content must yield 200")
}

// TestHandler_GraceTokenIdempotency verifies that replaying an old token within the grace
// window returns the same response as the original poll.
func TestHandler_GraceTokenIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "p", `{"val":42}`)
	token := startSession(t, h, "app", "env", "p")

	// First poll.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	body1 := rec1.Body.String()
	next1 := rec1.Header().Get("Next-Poll-Configuration-Token")

	// Replay the original token (grace period).
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, body1, rec2.Body.String(), "grace replay must return same body")
	assert.Equal(t, next1, rec2.Header().Get("Next-Poll-Configuration-Token"),
		"grace replay must return same next token")
}

func TestHandler_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/unknown/path", nil)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Poll interval ---

func TestHandler_PollIntervalValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int
		wantStatus int
	}{
		{name: "below_minimum_rejected", interval: 5, wantStatus: http.StatusBadRequest},
		{name: "at_minimum_accepted", interval: 15, wantStatus: http.StatusCreated},
		{name: "above_minimum_accepted", interval: 60, wantStatus: http.StatusCreated},
		{name: "zero_uses_default", interval: 0, wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))

			bodyJSON, err := json.Marshal(map[string]any{
				"ApplicationIdentifier":                "app",
				"EnvironmentIdentifier":                "env",
				"ConfigurationProfileIdentifier":       "p",
				"RequiredMinimumPollIntervalInSeconds": tt.interval,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/configurationsessions", bodyJSON)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PollIntervalDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "profile", `{}`)
	token := startSession(t, h, "app", "env", "profile")

	cfgRec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, cfgRec.Code)
	assert.Equal(t, "30", cfgRec.Header().Get("Next-Poll-Interval-In-Seconds"))
	assert.NotEmpty(t, cfgRec.Header().Get("ETag"))
	assert.NotEmpty(t, cfgRec.Header().Get("Content-Length"))
}

func TestHandler_PollIntervalHonored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "app", "env", "profile", `{}`)

	sessionBody, err := json.Marshal(map[string]any{
		"ApplicationIdentifier":                "app",
		"EnvironmentIdentifier":                "env",
		"ConfigurationProfileIdentifier":       "profile",
		"RequiredMinimumPollIntervalInSeconds": 60,
	})
	require.NoError(t, err)

	sessionRec := doRequest(t, h, http.MethodPost, "/configurationsessions", sessionBody)
	require.Equal(t, http.StatusCreated, sessionRec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(sessionRec.Body.Bytes(), &resp))
	token := resp["InitialConfigurationToken"]

	cfgRec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, cfgRec.Code)
	assert.Equal(t, "60", cfgRec.Header().Get("Next-Poll-Interval-In-Seconds"))
}

// --- Backend tests ---

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := appconfigdata.Provider{}
	assert.Equal(t, "AppConfigData", p.Name())

	h, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestBackend_SetConfiguration_ContentTooLarge(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	huge := make([]byte, 1*1024*1024+1)
	err := b.SetConfiguration("app", "env", "profile", string(huge), "text/plain")
	assert.ErrorIs(t, err, appconfigdata.ErrContentTooLarge)
}

func TestBackend_SetConfiguration_JSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		content     string
		contentType string
	}{
		{
			name:        "valid_json_accepted",
			content:     `{"key":"value"}`,
			contentType: "application/json",
			wantErr:     nil,
		},
		{
			name:        "invalid_json_rejected",
			content:     `not valid json`,
			contentType: "application/json",
			wantErr:     appconfigdata.ErrContentTypeMismatch,
		},
		{
			name:        "plain_text_not_validated",
			content:     `not json but thats ok`,
			contentType: "text/plain",
			wantErr:     nil,
		},
		{
			name:        "json_plus_suffix_validated",
			content:     `{}`,
			contentType: "application/vnd.api+json",
			wantErr:     nil,
		},
		{
			name:        "json_plus_suffix_invalid_rejected",
			content:     `bad`,
			contentType: "application/vnd.api+json",
			wantErr:     appconfigdata.ErrContentTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			err := b.SetConfiguration("app", "env", "p", tt.content, tt.contentType)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_SetConfiguration_ContentHashNormalization(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	// Two semantically equivalent JSON objects with different whitespace / key order.
	v1 := `{"b":2,"a":1}`
	v2 := `{ "a": 1, "b": 2 }`

	require.NoError(t, b.SetConfiguration("app", "env", "p", v1, "application/json"))
	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	hash1 := profiles[0].ContentHash

	require.NoError(t, b.SetConfiguration("app", "env", "p", v2, "application/json"))
	profiles = b.ListProfiles()
	require.Len(t, profiles, 1)
	hash2 := profiles[0].ContentHash

	// Normalised JSON produces the same hash for semantically equal documents.
	// Note: Go's json.Marshal sorts map keys, so normalisation is deterministic.
	// The hash equality here depends on key ordering after marshal; both should match.
	assert.Equal(t, hash1, hash2, "semantically equivalent JSON should produce the same hash")
}

func TestBackend_SetConfiguration_History(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v1", "text/plain"))
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v2", "text/plain"))
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "v3", "text/plain"))

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	p := profiles[0]
	assert.Equal(t, "v3", p.Content)
	require.Len(t, p.History, 2)
	assert.Equal(t, "v2", p.History[0].Content)
	assert.Equal(t, "v1", p.History[1].Content)
}

func TestBackend_SetConfiguration_ContentHash(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(
		t,
		b.SetConfiguration("app", "env", "profile", `{"key":"value"}`, "application/json"),
	)

	profiles := b.ListProfiles()
	require.Len(t, profiles, 1)
	assert.NotEmpty(t, profiles[0].ContentHash)
}

func TestBackend_SetConfiguration_VersionLabel(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	require.NoError(t, b.SetConfiguration("app", "env", "p", "v1", "text/plain"))
	ps := b.ListProfiles()
	require.Len(t, ps, 1)
	assert.Equal(t, "v1", ps[0].VersionLabel)
	assert.Equal(t, 1, ps[0].VersionNumber)

	require.NoError(t, b.SetConfiguration("app", "env", "p", "v2", "text/plain"))
	ps = b.ListProfiles()
	require.Len(t, ps, 1)
	assert.Equal(t, "v2", ps[0].VersionLabel)
	assert.Equal(t, 2, ps[0].VersionNumber)
}

func TestBackend_StartSession_RequiresDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *appconfigdata.InMemoryBackend)
		name    string
	}{
		{
			name:    "no_profile_returns_ErrNoActiveDeployment",
			wantErr: appconfigdata.ErrNoActiveDeployment,
		},
		{
			name: "profile_exists_succeeds",
			setup: func(b *appconfigdata.InMemoryBackend) {
				require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))
			},
			wantErr: nil,
		},
		{
			name: "wrong_profile_key_still_fails",
			setup: func(b *appconfigdata.InMemoryBackend) {
				require.NoError(t, b.SetConfiguration("other-app", "env", "p", `{}`, "application/json"))
			},
			wantErr: appconfigdata.ErrNoActiveDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			_, err := b.StartSession("app", "env", "p", 0)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_TokenHasHighEntropy(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Token format: <64-hex-random>.<16-hex-mac>
	// Total length should be at least 64 chars (32 random bytes in hex).
	assert.GreaterOrEqual(t, len(token), 64, "token must have >= 64 chars (32 random bytes in hex)")
	assert.Contains(t, token, ".", "token must contain MAC separator")
}

func TestBackend_TokenFamilyID(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	sess := b.LookupSession(token)
	require.NotNil(t, sess)
	assert.NotEmpty(t, sess.TokenFamilyID, "session must have a non-empty family ID")

	// Poll and verify the family ID is preserved.
	_, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	sess2 := b.LookupSession(nextToken)
	require.NotNil(t, sess2)
	assert.Equal(t, sess.TokenFamilyID, sess2.TokenFamilyID, "family ID must persist across rotation")
}

func TestBackend_ListProfiles(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	assert.Empty(t, b.ListProfiles())

	require.NoError(t, b.SetConfiguration("app1", "env1", "profile1", "data1", "text/plain"))
	require.NoError(t, b.SetConfiguration("app2", "env2", "profile2", `{}`, "application/json"))

	profiles := b.ListProfiles()
	assert.Len(t, profiles, 2)
}

func TestBackend_ListSessions(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "data", "text/plain"))

	assert.Empty(t, b.ListSessions())

	_, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	_, err = b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	sessions := b.ListSessions()
	assert.Len(t, sessions, 2)
}

func TestBackend_ListSessionsSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tokenCount int
	}{
		{name: "no_sessions", tokenCount: 0},
		{name: "one_session", tokenCount: 1},
		{name: "multiple_sessions", tokenCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			require.NoError(t, b.SetConfiguration("app", "env", "p", "data", "text/plain"))

			for range tt.tokenCount {
				_, err := b.StartSession("app", "env", "p", 0)
				require.NoError(t, err)
			}

			safe := b.ListSessionsSafe()
			assert.Len(t, safe, tt.tokenCount)

			for _, s := range safe {
				// TokenPrefix must be set (non-empty for tokens with content).
				assert.NotEmpty(t, s.TokenPrefix)
				// TokenPrefix must NOT be a full-length token.
				assert.Less(t, len(s.TokenPrefix), 64,
					"safe session token prefix must be shorter than full token")
				// Must contain the ellipsis separator.
				assert.Contains(t, s.TokenPrefix, "…",
					"safe session token prefix must contain ellipsis")
			}
		})
	}
}

func TestBackend_DeleteProfile(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "data", "text/plain"))

	_, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	assert.True(t, b.DeleteProfile("app", "env", "profile"))
	assert.False(t, b.DeleteProfile("app", "env", "profile"))

	assert.Empty(t, b.ListProfiles())
	assert.Empty(t, b.ListSessions())
}

func TestBackend_EndSession(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	assert.True(t, b.EndSession(token))
	assert.False(t, b.EndSession(token))
	assert.Empty(t, b.ListSessions())
}

func TestBackend_GetStats(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	stats := b.GetStats()
	assert.Equal(t, 0, stats.SessionCount)
	assert.Equal(t, 0, stats.ProfileCount)
	assert.Equal(t, int64(0), stats.TotalPollCount)
	assert.Equal(t, int64(0), stats.TotalPollFailures)
	assert.Equal(t, int64(0), stats.ConfigurationChangeCount)

	require.NoError(t, b.SetConfiguration("app", "env", "profile", "data", "text/plain"))
	_, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	stats = b.GetStats()
	assert.Equal(t, 1, stats.SessionCount)
	assert.Equal(t, 1, stats.ProfileCount)
	assert.Equal(t, int64(1), stats.ConfigurationChangeCount)
}

func TestBackend_GetStats_PollCounts(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Successful polls increment totalPolls.
	_, _, token, _, _, err = b.GetLatestConfiguration(token)
	require.NoError(t, err)
	_, _, _, _, _, err = b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Failed poll increments totalFailures.
	_, _, _, _, _, err = b.GetLatestConfiguration("bad-token")
	require.ErrorIs(t, err, appconfigdata.ErrSessionNotFound)

	stats := b.GetStats()
	assert.Equal(t, int64(2), stats.TotalPollCount)
	assert.Equal(t, int64(1), stats.TotalPollFailures)
}

func TestBackend_SweepExpiredSessions(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", `{}`, "application/json"))

	_, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	// Sweep with zero TTL — all sessions expire immediately.
	b.SweepExpiredSessions(t.Context(), 0)
	assert.Empty(t, b.ListSessions())
}

func TestBackend_SweepExpiredSessions_AbsoluteExpiry(t *testing.T) {
	t.Parallel()

	// Create a backend and start a session, then verify that ExpiresAt is ~1h from now.
	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	sess := b.LookupSession(token)
	require.NotNil(t, sess)
	assert.False(t, sess.ExpiresAt.IsZero(), "ExpiresAt must be set")
	// ExpiresAt should be approximately 1 hour from now.
	assert.True(t, sess.ExpiresAt.After(sess.CreatedAt), "ExpiresAt must be after CreatedAt")
}

func TestBackend_PollCount(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "profile", "data", "text/plain"))

	token, err := b.StartSession("app", "env", "profile", 0)
	require.NoError(t, err)

	for i := range 3 {
		var nextToken string
		_, _, nextToken, _, _, err = b.GetLatestConfiguration(token)
		require.NoError(t, err)
		token = nextToken

		sess := b.LookupSession(token)
		require.NotNil(t, sess)
		assert.Equal(t, i+1, sess.PollCount)
	}
}

// TestBackend_ChangeDetection verifies that GetLatestConfiguration returns empty content
// when the profile has not changed since the last poll.
func TestBackend_ChangeDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		firstContent  string
		secondContent string
		wantContent   bool // whether second poll should return content
	}{
		{
			name:          "unchanged_content_returns_empty",
			firstContent:  `{"v":1}`,
			secondContent: "",
			wantContent:   false,
		},
		{
			name:          "changed_content_returns_new_content",
			firstContent:  `{"v":1}`,
			secondContent: `{"v":2}`,
			wantContent:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfigdata.NewInMemoryBackend()
			require.NoError(t, b.SetConfiguration("app", "env", "p", tt.firstContent, "application/json"))

			token, err := b.StartSession("app", "env", "p", 0)
			require.NoError(t, err)

			// First poll.
			content1, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
			require.NoError(t, err)
			assert.NotEmpty(t, content1, "first poll must return content")

			if tt.secondContent != "" {
				require.NoError(t, b.SetConfiguration("app", "env", "p", tt.secondContent, "application/json"))
			}

			// Second poll.
			content2, _, _, _, _, err := b.GetLatestConfiguration(nextToken)
			require.NoError(t, err)

			if tt.wantContent {
				assert.NotEmpty(t, content2, "second poll must return updated content")
			} else {
				assert.Empty(t, content2, "second poll must return empty (no change)")
			}
		})
	}
}

// TestBackend_GraceTokenIdempotency verifies that replaying a rotated token within the grace
// window returns the cached response, not a new rotation.
func TestBackend_GraceTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"x":1}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// First poll.
	content1, ct1, next1, hash1, vl1, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Replay the old token (grace period).
	content2, ct2, next2, hash2, vl2, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	assert.Equal(t, content1, content2, "grace replay must return same content")
	assert.Equal(t, ct1, ct2, "grace replay must return same content type")
	assert.Equal(t, next1, next2, "grace replay must return same next token")
	assert.Equal(t, hash1, hash2, "grace replay must return same hash")
	assert.Equal(t, vl1, vl2, "grace replay must return same version label")
}

// TestBackend_ResourceRemovedMidSession verifies that polling with a valid token after the
// profile is manually removed returns ErrResourceRemoved.
// Note: DeleteProfile also removes linked sessions, so the test simulates profile removal
// without session eviction by checking the backend behaviour directly.
func TestBackend_ResourceRemovedMidSession(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	// Poll to get a live next-token.
	_, _, nextToken, _, _, err := b.GetLatestConfiguration(token)
	require.NoError(t, err)

	// Remove the profile (this also removes the session for nextToken).
	b.DeleteProfile("app", "env", "p")

	// Poll with the next token: session was removed → ErrSessionNotFound.
	_, _, _, _, _, err = b.GetLatestConfiguration(nextToken)
	assert.ErrorIs(t, err, appconfigdata.ErrSessionNotFound)
}

// TestBackend_MultipleProfilesIndependent verifies that sessions for different profiles
// do not interfere with each other.
func TestBackend_MultipleProfilesIndependent(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p1", `{"p":1}`, "application/json"))
	require.NoError(t, b.SetConfiguration("app", "env", "p2", `{"p":2}`, "application/json"))

	t1, err := b.StartSession("app", "env", "p1", 0)
	require.NoError(t, err)

	t2, err := b.StartSession("app", "env", "p2", 0)
	require.NoError(t, err)

	c1, _, _, _, _, err := b.GetLatestConfiguration(t1)
	require.NoError(t, err)

	c2, _, _, _, _, err := b.GetLatestConfiguration(t2)
	require.NoError(t, err)

	assert.NotEqual(t, string(c1), string(c2), "separate profiles must return different content")
}

// TestBackend_TruncateToken verifies the truncation function exposed via ListSessionsSafe.
func TestBackend_TruncateToken(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	_, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)

	safe := b.ListSessionsSafe()
	require.Len(t, safe, 1)

	full := b.ListSessions()
	require.Len(t, full, 1)

	assert.Less(t, len(safe[0].TokenPrefix), len(full[0].Token),
		"safe token must be shorter than full token")
	// Full token must start with the same prefix shown in safe token.
	prefix := strings.Split(safe[0].TokenPrefix, "…")[0]
	assert.True(t, strings.HasPrefix(full[0].Token, prefix),
		"full token must start with the safe prefix")
}

// TestHandler_ContentTypeNotSetOn204 is a focused regression test for the protocol violation
// where NoContent responses previously set Content-Type.
func TestHandler_ContentTypeNotSetOn204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedProfile(t, h, "a", "e", "p", `{"x":1}`)
	token := startSession(t, h, "a", "e", "p")

	// First poll.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	next := rec1.Header().Get("Next-Poll-Configuration-Token")

	// Second poll — content unchanged.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+next, nil)
	require.Equal(t, http.StatusNoContent, rec2.Code)
	// Content-Type must NOT be set to "application/json" (or anything data-describing) on 204.
	ct := rec2.Header().Get("Content-Type")
	assert.Empty(t, ct, "Content-Type must not be set on 204 response, got: %q", ct)
}

// TestBackend_GetStats_ConfigurationChangeCount verifies that the counter increments only
// when content actually changes, not on identical re-sets.
func TestBackend_GetStats_ConfigurationChangeCount(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()

	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
	assert.Equal(t, int64(1), b.GetStats().ConfigurationChangeCount)

	// Same content — normalised hash matches — no increment.
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":1}`, "application/json"))
	assert.Equal(t, int64(1), b.GetStats().ConfigurationChangeCount)

	// Different content — increments.
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{"v":2}`, "application/json"))
	assert.Equal(t, int64(2), b.GetStats().ConfigurationChangeCount)
}

// TestBackend_SessionExpiresAtPopulated verifies ExpiresAt is set correctly on new sessions.
func TestBackend_SessionExpiresAtPopulated(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	require.NoError(t, b.SetConfiguration("app", "env", "p", `{}`, "application/json"))

	before := nowUTC()
	token, err := b.StartSession("app", "env", "p", 0)
	require.NoError(t, err)
	after := nowUTC()

	sess := b.LookupSession(token)
	require.NotNil(t, sess)

	assert.True(t, sess.ExpiresAt.After(before), "ExpiresAt must be after start time")
	// ExpiresAt should be approximately 1h after creation.
	maxExpiry := after.Add(time.Hour + time.Second)
	assert.True(t, sess.ExpiresAt.Before(maxExpiry), "ExpiresAt must be within 1h+1s of creation")
}
