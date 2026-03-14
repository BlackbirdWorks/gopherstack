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

// TestDashboard_Xray_Index covers the X-Ray dashboard index handler.
func TestDashboard_Xray_Index(t *testing.T) {
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
			wantContains: "X-Ray Groups",
		},
		{
			name: "index shows created group",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.XrayHandler.Backend.CreateGroup("my-group", "")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "my-group",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No X-Ray groups found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/xray", nil)
			w := httptest.NewRecorder()
			s.Echo.ServeHTTP(w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_Xray_CreateGroup covers the create group handler.
func TestDashboard_Xray_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues   url.Values
		name         string
		wantCode     int
		wantLocation string
	}{
		{
			name:         "creates group and redirects",
			formValues:   url.Values{"group_name": {"new-group"}},
			wantCode:     http.StatusSeeOther,
			wantLocation: "/dashboard/xray",
		},
		{
			name:       "missing group_name returns 400",
			formValues: url.Values{},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "duplicate group returns 400",
			formValues: url.Values{"group_name": {"dup-group"}},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.name == "duplicate group returns 400" {
				_, err := s.XrayHandler.Backend.CreateGroup("dup-group", "")
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/xray/create-group",
				strings.NewReader(tt.formValues.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			s.Echo.ServeHTTP(w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}

// TestDashboard_Xray_DeleteGroup covers the delete group handler.
func TestDashboard_Xray_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		formValues   url.Values
		name         string
		wantCode     int
		wantLocation string
	}{
		{
			name: "deletes group and redirects",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.XrayHandler.Backend.CreateGroup("to-delete", "")
				require.NoError(t, err)
			},
			formValues:   url.Values{"group_name": {"to-delete"}},
			wantCode:     http.StatusSeeOther,
			wantLocation: "/dashboard/xray",
		},
		{
			name:       "missing group_name returns 400",
			formValues: url.Values{},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			formValues: url.Values{"group_name": {"no-such-group"}},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/xray/delete-group",
				strings.NewReader(tt.formValues.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			s.Echo.ServeHTTP(w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}
