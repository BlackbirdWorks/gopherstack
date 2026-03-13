package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// TestDashboard_SESv2_Index tests the SES v2 dashboard index page.
func TestDashboard_SESv2_Index(t *testing.T) {
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
			wantContains: "SES v2",
		},
		{
			name: "shows created email identity",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateEmailIdentity("dashboard@example.com")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard@example.com",
		},
		{
			name: "shows created configuration set",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateConfigurationSet("my-config-set")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "my-config-set",
		},
		{
			name:         "renders email identities section",
			wantCode:     http.StatusOK,
			wantContains: "Email Identities",
		},
		{
			name:         "renders configuration sets section",
			wantCode:     http.StatusOK,
			wantContains: "Configuration Sets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/sesv2", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_SESv2_Index_NilOps tests the SES v2 dashboard index when SESv2Ops is nil.
func TestDashboard_SESv2_Index_NilOps(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	req := httptest.NewRequest(http.MethodGet, "/dashboard/sesv2", nil)
	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "SES v2")
}

// TestDashboard_SESv2_CreateIdentity tests the create identity POST handler.
func TestDashboard_SESv2_CreateIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		verify       func(*testing.T, *teststack.Stack)
		name         string
		identity     string
		wantLocation string
		wantCode     int
	}{
		{
			name:         "creates email identity and redirects",
			identity:     "new@example.com",
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/sesv2",
			verify: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.GetEmailIdentity("new@example.com")
				require.NoError(t, err, "identity should exist in backend after creation")
			},
		},
		{
			name:     "empty identity returns bad request",
			identity: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate identity returns bad request",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateEmailIdentity("dup@example.com")
				require.NoError(t, err)
			},
			identity: "dup@example.com",
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
			if tt.identity != "" {
				form.Set("identity", tt.identity)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/identity/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
			if tt.verify != nil {
				tt.verify(t, s)
			}
		})
	}
}

// TestDashboard_SESv2_CreateIdentity_NilOps tests the create identity handler when SESv2Ops is nil.
func TestDashboard_SESv2_CreateIdentity_NilOps(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	form := url.Values{"identity": {"test@example.com"}}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/identity/create",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestDashboard_SESv2_DeleteIdentity tests the delete identity POST handler.
func TestDashboard_SESv2_DeleteIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		verify       func(*testing.T, *teststack.Stack)
		name         string
		identity     string
		wantLocation string
		wantCode     int
	}{
		{
			name: "deletes existing identity and redirects",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateEmailIdentity("todelete@example.com")
				require.NoError(t, err)
			},
			identity:     "todelete@example.com",
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/sesv2",
			verify: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.GetEmailIdentity("todelete@example.com")
				require.Error(t, err, "identity should be removed from backend after deletion")
			},
		},
		{
			name:     "empty identity returns bad request",
			identity: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent identity returns not found",
			identity: "nobody@example.com",
			wantCode: http.StatusNotFound,
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
			if tt.identity != "" {
				form.Set("identity", tt.identity)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/identity/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
			if tt.verify != nil {
				tt.verify(t, s)
			}
		})
	}
}

// TestDashboard_SESv2_DeleteIdentity_NilOps tests the delete identity handler when SESv2Ops is nil.
func TestDashboard_SESv2_DeleteIdentity_NilOps(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	form := url.Values{"identity": {"test@example.com"}}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/identity/delete",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestDashboard_SESv2_CreateConfigSet tests the create configuration set POST handler.
func TestDashboard_SESv2_CreateConfigSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		verify       func(*testing.T, *teststack.Stack)
		name         string
		setName      string
		wantLocation string
		wantCode     int
	}{
		{
			name:         "creates configuration set and redirects",
			setName:      "new-config-set",
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/sesv2",
			verify: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.GetConfigurationSet("new-config-set")
				require.NoError(t, err, "configuration set should exist in backend after creation")
			},
		},
		{
			name:     "empty name returns bad request",
			setName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate configuration set returns bad request",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateConfigurationSet("dup-config-set")
				require.NoError(t, err)
			},
			setName:  "dup-config-set",
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
			if tt.setName != "" {
				form.Set("name", tt.setName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/configuration-set/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
			if tt.verify != nil {
				tt.verify(t, s)
			}
		})
	}
}

// TestDashboard_SESv2_CreateConfigSet_NilOps tests the create config set handler when SESv2Ops is nil.
func TestDashboard_SESv2_CreateConfigSet_NilOps(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	form := url.Values{"name": {"my-set"}}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/configuration-set/create",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestDashboard_SESv2_DeleteConfigSet tests the delete configuration set POST handler.
func TestDashboard_SESv2_DeleteConfigSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		verify       func(*testing.T, *teststack.Stack)
		name         string
		setName      string
		wantLocation string
		wantCode     int
	}{
		{
			name: "deletes existing configuration set and redirects",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.CreateConfigurationSet("todelete-set")
				require.NoError(t, err)
			},
			setName:      "todelete-set",
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/sesv2",
			verify: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SESv2Handler.Backend.GetConfigurationSet("todelete-set")
				require.Error(t, err, "configuration set should be removed from backend after deletion")
			},
		},
		{
			name:     "empty name returns bad request",
			setName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent configuration set returns not found",
			setName:  "nonexistent-set",
			wantCode: http.StatusNotFound,
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
			if tt.setName != "" {
				form.Set("name", tt.setName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/configuration-set/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
			if tt.verify != nil {
				tt.verify(t, s)
			}
		})
	}
}

// TestDashboard_SESv2_DeleteConfigSet_NilOps tests the delete config set handler when SESv2Ops is nil.
func TestDashboard_SESv2_DeleteConfigSet_NilOps(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	form := url.Values{"name": {"my-set"}}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sesv2/configuration-set/delete",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
