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

// TestDashboard_Wafv2_Index covers the WAFv2 dashboard index handler.
func TestDashboard_Wafv2_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "no_handler renders page",
			wantCode:     http.StatusOK,
			wantContains: "WAFv2",
		},
		{
			name: "with_handler_and_data shows web ACL",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.Wafv2Handler.Backend.CreateWebACL(
					"test-acl",
					"REGIONAL",
					"",
					"ALLOW",
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "test-acl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/wafv2", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_Wafv2_Create covers the WAFv2 create handler.
func TestDashboard_Wafv2_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "success",
			formValues: map[string]string{
				"name":           "new-acl",
				"scope":          "REGIONAL",
				"default_action": "ALLOW",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name: "missing_name returns bad request",
			formValues: map[string]string{
				"scope": "REGIONAL",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_scope returns bad request",
			formValues: map[string]string{
				"name": "my-acl",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			for k, v := range tt.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/wafv2/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_Wafv2_Delete covers the WAFv2 delete handler.
func TestDashboard_Wafv2_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				acl, err := s.Wafv2Handler.Backend.CreateWebACL(
					"to-delete",
					"REGIONAL",
					"",
					"ALLOW",
					nil,
				)
				require.NoError(t, err)

				return acl.ID
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name: "missing id returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "nonexistent id returns not found",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return "nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)
			id := tt.setup(t, s)

			form := url.Values{}
			if id != "" {
				form.Set("id", id)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/wafv2/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
