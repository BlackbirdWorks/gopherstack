package emrserverless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

const sessionRoleARN = "arn:aws:iam::000000000000:role/interactive-session"

func createStartedApp(t *testing.T, h *emrserverless.Handler) string {
	t.Helper()

	appID := createApp(t, h, "interactive-app")
	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/start", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	return appID
}

func startSession(t *testing.T, h *emrserverless.Handler, appID, token string) (string, string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/sessions", map[string]any{
		"clientToken":        token,
		"executionRoleArn":   sessionRoleARN,
		"name":               "interactive",
		"idleTimeoutMinutes": 30,
		"tags":               map[string]string{"purpose": "notebook"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]string
	mustUnmarshal(t, rec, &out)
	require.NotEmpty(t, out["sessionId"])

	return out["sessionId"], out["arn"]
}

func TestHandler_SessionOperations(t *testing.T) {
	t.Parallel()

	type setupResult struct {
		appID     string
		sessionID string
	}
	tests := []struct {
		setup      func(*testing.T, *emrserverless.Handler) setupResult
		path       func(setupResult) string
		body       map[string]any
		name       string
		method     string
		wantField  string
		wantStatus int
	}{
		{
			name: "start_session",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				return setupResult{appID: createStartedApp(t, h)}
			},
			method: http.MethodPost,
			path:   func(r setupResult) string { return "/applications/" + r.appID + "/sessions" },
			body: map[string]any{
				"clientToken":      "new-token",
				"executionRoleArn": sessionRoleARN,
			},
			wantStatus: http.StatusOK,
			wantField:  "sessionId",
		},
		{
			name: "get_session",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				appID := createStartedApp(t, h)
				sessionID, _ := startSession(t, h, appID, "get-token")

				return setupResult{appID: appID, sessionID: sessionID}
			},
			method:     http.MethodGet,
			path:       func(r setupResult) string { return "/applications/" + r.appID + "/sessions/" + r.sessionID },
			wantStatus: http.StatusOK,
			wantField:  "session",
		},
		{
			name: "list_sessions",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				appID := createStartedApp(t, h)
				startSession(t, h, appID, "list-token")

				return setupResult{appID: appID}
			},
			method:     http.MethodGet,
			path:       func(r setupResult) string { return "/applications/" + r.appID + "/sessions" },
			wantStatus: http.StatusOK,
			wantField:  "sessions",
		},
		{
			name: "get_session_endpoint",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				appID := createStartedApp(t, h)
				sessionID, _ := startSession(t, h, appID, "endpoint-token")

				return setupResult{appID: appID, sessionID: sessionID}
			},
			method: http.MethodGet,
			path: func(r setupResult) string {
				return "/applications/" + r.appID + "/sessions/" + r.sessionID + "/endpoint"
			},
			wantStatus: http.StatusOK,
			wantField:  "authToken",
		},
		{
			name: "get_resource_dashboard",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				appID := createStartedApp(t, h)
				sessionID, _ := startSession(t, h, appID, "dash-token")

				return setupResult{appID: appID, sessionID: sessionID}
			},
			method: http.MethodGet,
			path: func(r setupResult) string {
				return fmt.Sprintf(
					"/applications/%s/dashboard?resourceId=%s&resourceType=SESSION",
					r.appID,
					r.sessionID,
				)
			},
			wantStatus: http.StatusOK,
			wantField:  "url",
		},
		{
			name: "terminate_session",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				appID := createStartedApp(t, h)
				sessionID, _ := startSession(t, h, appID, "terminate-token")

				return setupResult{appID: appID, sessionID: sessionID}
			},
			method:     http.MethodDelete,
			path:       func(r setupResult) string { return "/applications/" + r.appID + "/sessions/" + r.sessionID },
			wantStatus: http.StatusOK,
			wantField:  "sessionId",
		},
		{
			name: "start_requires_started_application",
			setup: func(t *testing.T, h *emrserverless.Handler) setupResult {
				t.Helper()

				return setupResult{appID: createApp(t, h, "stopped-app")}
			},
			method: http.MethodPost,
			path:   func(r setupResult) string { return "/applications/" + r.appID + "/sessions" },
			body: map[string]any{
				"clientToken":      "invalid-state-token",
				"executionRoleArn": sessionRoleARN,
			},
			wantStatus: http.StatusBadRequest,
			wantField:  "code",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			result := tt.setup(t, h)
			rec := doRequest(t, h, tt.method, tt.path(result), tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Contains(t, out, tt.wantField)
		})
	}
}

func TestHandler_SessionIdempotencyAndTermination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createStartedApp(t, h)
	firstID, sessionARN := startSession(t, h, appID, "stable-token")
	secondID, _ := startSession(t, h, appID, "stable-token")
	assert.Equal(t, firstID, secondID)

	tagPath := "/tags/" + url.PathEscape(sessionARN)
	rec := doRequest(t, h, http.MethodPost, tagPath, map[string]any{"tags": map[string]string{"env": "dev"}})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"env":"dev"`)

	rec = doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/sessions/"+firstID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doRequest(t, h, http.MethodGet, "/applications/"+appID+"/sessions/"+firstID+"/endpoint", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+appID+"/dashboard?resourceId="+firstID+"&resourceType=SESSION",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_SessionFilteringAndPersistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createStartedApp(t, h)
	sessionID, _ := startSession(t, h, appID, "persist-token")
	doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/sessions/"+sessionID, nil)
	startSession(t, h, appID, "running-token")

	rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/sessions?states=TERMINATED", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var filtered struct {
		Sessions []map[string]any `json:"sessions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &filtered))
	require.Len(t, filtered.Sessions, 1)
	assert.Equal(t, emrserverless.SessionStateTerminated, filtered.Sessions[0]["state"])

	restored := newTestHandler(t)
	require.NoError(t, restored.Restore(t.Context(), h.Snapshot(t.Context())))
	rec = doRequest(t, restored, http.MethodGet, "/applications/"+appID+"/sessions/"+sessionID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), emrserverless.SessionStateTerminated)
}

func TestHandler_SessionRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		path   string
		want   string
	}{
		{method: http.MethodPost, path: "/applications/a/sessions", want: "StartSession"},
		{method: http.MethodGet, path: "/applications/a/sessions", want: "ListSessions"},
		{method: http.MethodGet, path: "/applications/a/sessions/s", want: "GetSession"},
		{method: http.MethodDelete, path: "/applications/a/sessions/s", want: "TerminateSession"},
		{method: http.MethodGet, path: "/applications/a/sessions/s/endpoint", want: "GetSessionEndpoint"},
		{method: http.MethodGet, path: "/applications/a/dashboard", want: "GetResourceDashboard"},
	}
	h := newTestHandler(t)
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			c := e.NewContext(httptest.NewRequest(tt.method, tt.path, nil), httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}
