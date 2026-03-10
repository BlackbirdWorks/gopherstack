package appconfigdata_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

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
	assert.Equal(t, []string{"StartConfigurationSession", "GetLatestConfiguration"}, h.ChaosOperations())
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
		{name: "unknown", method: http.MethodDelete, path: "/configurationsessions", want: "Unknown"},
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

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/configuration?configuration_token=my-token-123", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "my-token-123", h.ExtractResource(c))
}

func TestHandler_StartConfigurationSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
		wantToken  bool
	}{
		{
			name: "success",
			body: []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			),
			wantStatus: http.StatusCreated,
			wantToken:  true,
		},
		{
			name:       "missing_application",
			body:       []byte(`{"EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_environment",
			body:       []byte(`{"ApplicationIdentifier":"my-app","ConfigurationProfileIdentifier":"my-profile"}`),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
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

func TestHandler_GetLatestConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     string
		contentType string
		wantBody    string
		wantStatus  int
		hasProfile  bool
	}{
		{
			name:        "with_configuration",
			hasProfile:  true,
			content:     `{"featureFlag":true}`,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
			wantBody:    `{"featureFlag":true}`,
		},
		{
			name:       "session_without_profile",
			hasProfile: false,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "invalid_token",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "invalid_token" {
				rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token=bad-token", nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			if tt.hasProfile {
				h.Backend.SetConfiguration("my-app", "prod", "my-profile", tt.content, tt.contentType)
			}

			// Start a session to get an initial token.
			sessionBody := []byte(
				`{"ApplicationIdentifier":"my-app","EnvironmentIdentifier":"prod","ConfigurationProfileIdentifier":"my-profile"}`,
			)
			sessionRec := doRequest(t, h, http.MethodPost, "/configurationsessions", sessionBody)
			require.Equal(t, http.StatusCreated, sessionRec.Code)

			var sessionResp map[string]string
			require.NoError(t, json.Unmarshal(sessionRec.Body.Bytes(), &sessionResp))
			token := sessionResp["InitialConfigurationToken"]
			require.NotEmpty(t, token)

			// Get configuration with the token.
			rec := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}

			if tt.wantStatus == http.StatusOK || tt.wantStatus == http.StatusNoContent {
				nextToken := rec.Header().Get("Next-Poll-Configuration-Token")
				assert.NotEmpty(t, nextToken)
				assert.NotEqual(t, token, nextToken)
			}
		})
	}
}

func TestHandler_TokenRotation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.SetConfiguration("app", "env", "profile", `{"v":1}`, "application/json")

	// Start session.
	sessionBody := []byte(
		`{"ApplicationIdentifier":"app","EnvironmentIdentifier":"env","ConfigurationProfileIdentifier":"profile"}`,
	)
	sessionRec := doRequest(t, h, http.MethodPost, "/configurationsessions", sessionBody)
	require.Equal(t, http.StatusCreated, sessionRec.Code)

	var sessionResp map[string]string
	require.NoError(t, json.Unmarshal(sessionRec.Body.Bytes(), &sessionResp))
	token := sessionResp["InitialConfigurationToken"]

	// First poll succeeds.
	rec1 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	nextToken := rec1.Header().Get("Next-Poll-Configuration-Token")
	assert.NotEmpty(t, nextToken)

	// Old token no longer valid.
	rec2 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+token, nil)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// New token works.
	rec3 := doRequest(t, h, http.MethodGet, "/configuration?configuration_token="+nextToken, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_EmptyToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/configuration", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestBackend_ListProfiles(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	assert.Empty(t, b.ListProfiles())

	b.SetConfiguration("app1", "env1", "profile1", "data1", "text/plain")
	b.SetConfiguration("app2", "env2", "profile2", "data2", "application/json")

	profiles := b.ListProfiles()
	assert.Len(t, profiles, 2)
}

func TestBackend_ListSessions(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	b.SetConfiguration("app", "env", "profile", "data", "text/plain")

	assert.Empty(t, b.ListSessions())

	_, err := b.StartSession("app", "env", "profile")
	require.NoError(t, err)

	_, err = b.StartSession("app", "env", "profile")
	require.NoError(t, err)

	sessions := b.ListSessions()
	assert.Len(t, sessions, 2)
}

func TestBackend_DeleteProfile(t *testing.T) {
	t.Parallel()

	b := appconfigdata.NewInMemoryBackend()
	b.SetConfiguration("app", "env", "profile", "data", "text/plain")

	_, err := b.StartSession("app", "env", "profile")
	require.NoError(t, err)

	assert.True(t, b.DeleteProfile("app", "env", "profile"))
	assert.False(t, b.DeleteProfile("app", "env", "profile"))

	assert.Empty(t, b.ListProfiles())
	assert.Empty(t, b.ListSessions())
}

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := appconfigdata.Provider{}
	assert.Equal(t, "AppConfigData", p.Name())

	h, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, h)
}
