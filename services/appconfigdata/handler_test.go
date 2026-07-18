package appconfigdata_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

func nowUTC() time.Time { return time.Now().UTC() }

func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

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

// decodeErrorBody parses a JSON error response body and returns __type and message.
func decodeErrorBody(t *testing.T, body string) (string, string) {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &m), "error body must be valid JSON")

	errType, _ := m["__type"].(string)
	errMsg, _ := m["message"].(string)

	return errType, errMsg
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

// --- Provider ---

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := appconfigdata.Provider{}
	assert.Equal(t, "AppConfigData", p.Name())

	h, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, h)
}
