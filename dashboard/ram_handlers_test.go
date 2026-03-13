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

// TestDashboard_RAM_Index covers the RAM dashboard index handler.
func TestDashboard_RAM_Index(t *testing.T) {
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
			wantContains: "RAM Resource Shares",
		},
		{
			name: "index renders created share",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.RAMHandler.Backend.CreateResourceShare(
					"dashboard-test-share",
					true,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-share",
		},
		{
			name:         "index shows no resource shares message when empty",
			wantCode:     http.StatusOK,
			wantContains: "No resource shares found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/ram", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_RAM_Create covers the RAM resource share create handler.
func TestDashboard_RAM_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "create share redirects",
			formValues: map[string]string{
				"name": "new-share",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
			formValues: map[string]string{},
			wantCode:   http.StatusBadRequest,
		},
		{
			name: "duplicate name returns bad request",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.RAMHandler.Backend.CreateResourceShare("dup-share", true, nil, nil, nil)
				require.NoError(t, err)
			},
			formValues: map[string]string{
				"name": "dup-share",
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

			req := httptest.NewRequest(http.MethodPost, "/dashboard/ram/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_RAM_Delete covers the RAM resource share delete handler.
func TestDashboard_RAM_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing share redirects",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				rs, err := s.RAMHandler.Backend.CreateResourceShare("del-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name: "missing arn returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found share returns 404",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/does-not-exist"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)
			shareARN := tt.setup(t, s)

			form := url.Values{}
			if shareARN != "" {
				form.Set("arn", shareARN)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/ram/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
