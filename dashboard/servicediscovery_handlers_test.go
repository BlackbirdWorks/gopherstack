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

// TestDashboard_ServiceDiscovery_Index covers the Cloud Map dashboard index handler.
func TestDashboard_ServiceDiscovery_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "renders page header",
			wantCode:     http.StatusOK,
			wantContains: "Service Discovery",
		},
		{
			name: "renders namespace",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.ServiceDiscoveryHandler.Backend.CreateHTTPNamespace("my-namespace", "", nil)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "my-namespace",
		},
		{
			name:         "shows empty state when no namespaces",
			wantCode:     http.StatusOK,
			wantContains: "No namespaces created yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/servicediscovery", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_ServiceDiscovery_Index_NilOps covers servicediscoveryIndex when ServiceDiscoveryOps is nil.
func TestDashboard_ServiceDiscovery_Index_NilOps(t *testing.T) {
	t.Parallel()

	s := newStack(t)
	s.Dashboard.ServiceDiscoveryOps = nil

	req := httptest.NewRequest(http.MethodGet, "/dashboard/servicediscovery", nil)
	w := httptest.NewRecorder()
	serveHandler(s.Dashboard, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
}

// TestDashboard_ServiceDiscovery_CreateNamespace covers the namespace creation handler.
func TestDashboard_ServiceDiscovery_CreateNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name:       "create namespace redirects",
			formValues: map[string]string{"name": "my-namespace"},
			wantCode:   http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
			formValues: map[string]string{},
			wantCode:   http.StatusBadRequest,
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

			req := httptest.NewRequest(http.MethodPost, "/dashboard/servicediscovery/namespace",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_ServiceDiscovery_CreateNamespace_NilOps covers the handler when ServiceDiscoveryOps is nil.
func TestDashboard_ServiceDiscovery_CreateNamespace_NilOps(t *testing.T) {
	t.Parallel()

	s := newStack(t)
	s.Dashboard.ServiceDiscoveryOps = nil

	form := url.Values{"name": {"my-namespace"}}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/servicediscovery/namespace",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(s.Dashboard, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
