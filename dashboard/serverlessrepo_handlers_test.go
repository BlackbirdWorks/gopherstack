package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// TestDashboard_ServerlessRepo_Index covers the Serverless Application Repository dashboard index handler.
func TestDashboard_ServerlessRepo_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders page header",
			wantCode:     http.StatusOK,
			wantContains: "Serverless Application Repository",
		},
		{
			name: "index renders created application",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.ServerlessRepoHandler.Backend.CreateApplication(
					"dashboard-test-app",
					"A test application",
					"test-author",
					"",
					"1.0.0",
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-app",
		},
		{
			name:         "index shows no applications message when empty",
			wantCode:     http.StatusOK,
			wantContains: "No applications found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/serverlessrepo", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_ServerlessRepo_Create covers the SAR application create handler.
func TestDashboard_ServerlessRepo_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "create application redirects",
			formValues: map[string]string{
				"name": "new-app",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
			formValues: map[string]string{},
			wantCode:   http.StatusBadRequest,
		},
		{
			name: "duplicate application name returns bad request",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.ServerlessRepoHandler.Backend.CreateApplication("dup-app", "", "", "", "", nil)
				require.NoError(t, err)
			},
			formValues: map[string]string{
				"name": "dup-app",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			form := url.Values{}
			for k, v := range tt.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/serverlessrepo/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_ServerlessRepo_Delete covers the SAR application delete handler.
func TestDashboard_ServerlessRepo_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing application redirects",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				a, err := s.ServerlessRepoHandler.Backend.CreateApplication("del-app", "", "", "", "", nil)
				require.NoError(t, err)

				return a.Name
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name: "missing name returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found application returns 404",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return "does-not-exist"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)
			appName := tt.setup(t, s)

			form := url.Values{}
			if appName != "" {
				form.Set("name", appName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/serverlessrepo/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
