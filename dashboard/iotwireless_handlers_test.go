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
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// TestIoTWirelessHandlers_Index covers the IoT Wireless dashboard index page.
func TestIoTWirelessHandlers_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "renders empty state with no profiles",
			wantCode:     http.StatusOK,
			wantContains: "No service profiles found",
		},
		{
			name: "renders created service profile",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.IoTWirelessHandler.Backend.CreateServiceProfile(
					config.DefaultAccountID,
					config.DefaultRegion,
					"dashboard-test-profile",
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-profile",
		},
		{
			name:         "renders IoT Wireless header",
			wantCode:     http.StatusOK,
			wantContains: "IoT Wireless",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/iotwireless", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestIoTWirelessHandlers_CreateDelete tests create and delete service profile form actions.
func TestIoTWirelessHandlers_CreateDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues   url.Values
		name         string
		path         string
		method       string
		wantLocation string
		wantCode     int
	}{
		{
			name:         "create redirects",
			method:       http.MethodPost,
			path:         "/dashboard/iotwireless/service-profile/create",
			formValues:   url.Values{"name": {"my-profile"}},
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/iotwireless",
		},
		{
			name:       "create with empty name returns bad request",
			method:     http.MethodPost,
			path:       "/dashboard/iotwireless/service-profile/create",
			formValues: url.Values{"name": {""}},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "delete with empty id returns bad request",
			method:     http.MethodPost,
			path:       "/dashboard/iotwireless/service-profile/delete",
			formValues: url.Values{"id": {""}},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			body := strings.NewReader(tt.formValues.Encode())
			req := httptest.NewRequest(tt.method, tt.path, body)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}

// TestIoTWirelessHandlers_DeleteExisting tests that deleting an existing profile redirects
// and the profile is no longer present in the backend.
func TestIoTWirelessHandlers_DeleteExisting(t *testing.T) {
	t.Parallel()

	s := newStack(t)

	// Pre-create a profile.
	sp, err := s.IoTWirelessHandler.Backend.CreateServiceProfile(
		config.DefaultAccountID, config.DefaultRegion, "to-delete", nil,
	)
	require.NoError(t, err)

	body := strings.NewReader(url.Values{"id": {sp.ID}}.Encode())
	req := httptest.NewRequest(http.MethodPost, "/dashboard/iotwireless/service-profile/delete", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(s.Dashboard, w, req)

	assert.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/dashboard/iotwireless", w.Header().Get("Location"))

	// Verify the profile is gone.
	profiles := s.IoTWirelessHandler.Backend.ListServiceProfiles(config.DefaultAccountID, config.DefaultRegion)
	assert.Empty(t, profiles, "deleted profile should not appear in list")
}
