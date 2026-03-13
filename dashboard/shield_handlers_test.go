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

// TestDashboard_Shield_Index covers the Shield dashboard index handler.
func TestDashboard_Shield_Index(t *testing.T) {
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
			wantContains: "Shield Advanced",
		},
		{
			name: "index shows active subscription state",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				err := s.ShieldHandler.Backend.CreateSubscription()
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "Subscription Active",
		},
		{
			name: "index shows created protection",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				err := s.ShieldHandler.Backend.CreateSubscription()
				require.NoError(t, err)

				_, err = s.ShieldHandler.Backend.CreateProtection(
					"dashboard-test-protection",
					"arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-protection",
		},
		{
			name:         "index shows enable button when no subscription",
			wantCode:     http.StatusOK,
			wantContains: "Enable Shield Advanced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/shield", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_Shield_Subscribe covers the Shield subscribe handler.
func TestDashboard_Shield_Subscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "subscribe redirects",
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "subscribe is idempotent",
			wantCode: http.StatusSeeOther,
		},
	}

	s := newStack(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/dashboard/shield/subscribe", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_Shield_Protect covers the Shield protect handler.
func TestDashboard_Shield_Protect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "create protection redirects",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				err := s.ShieldHandler.Backend.CreateSubscription()
				require.NoError(t, err)
			},
			formValues: map[string]string{
				"name":         "test-protection",
				"resource_arn": "arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
			formValues: map[string]string{"resource_arn": "arn:aws:ec2:us-east-1:123:eip/eipalloc-1"},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing resource arn returns bad request",
			formValues: map[string]string{"name": "my-protection"},
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

			form := url.Values{}
			for k, v := range tt.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/shield/protect",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_Shield_Delete covers the Shield delete handler.
func TestDashboard_Shield_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing protection redirects",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				err := s.ShieldHandler.Backend.CreateSubscription()
				require.NoError(t, err)

				p, err := s.ShieldHandler.Backend.CreateProtection(
					"to-delete",
					"arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					nil,
				)
				require.NoError(t, err)

				return p.ID
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

			req := httptest.NewRequest(http.MethodPost, "/dashboard/shield/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
