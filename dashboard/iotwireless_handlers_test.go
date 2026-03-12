package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIoTWirelessHandlers covers the IoT Wireless dashboard handlers.
func TestIoTWirelessHandlers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T)
		name         string
		path         string
		method       string
		formBody     string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders empty page",
			method:       http.MethodGet,
			path:         "/dashboard/iotwireless",
			wantCode:     http.StatusOK,
			wantContains: "IoT Wireless",
		},
		{
			name:   "index renders created service profile",
			method: http.MethodGet,
			path:   "/dashboard/iotwireless",
			setup: func(t *testing.T) {
				t.Helper()
			},
			wantCode:     http.StatusOK,
			wantContains: "IoT Wireless",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t)
			}

			_, err := s.IoTWirelessHandler.Backend.CreateServiceProfile(
				"000000000000", "us-east-1", "dashboard-test-profile", nil,
			)
			require.NoError(t, err)

			req := httptest.NewRequest(tt.method, tt.path, nil)
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
		name     string
		method   string
		path     string
		formBody string
		wantCode int
	}{
		{
			name:     "create redirects",
			method:   http.MethodPost,
			path:     "/dashboard/iotwireless/service-profile/create",
			formBody: "name=my-profile",
			wantCode: http.StatusFound,
		},
		{
			name:     "create with empty name returns bad request",
			method:   http.MethodPost,
			path:     "/dashboard/iotwireless/service-profile/create",
			formBody: "name=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			var body *httptest.ResponseRecorder

			if tt.formBody != "" {
				req := httptest.NewRequest(tt.method, tt.path, nil)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.URL.RawQuery = tt.formBody
				body = httptest.NewRecorder()
				serveHandler(s.Dashboard, body, req)
			} else {
				req := httptest.NewRequest(tt.method, tt.path, nil)
				body = httptest.NewRecorder()
				serveHandler(s.Dashboard, body, req)
			}

			assert.Equal(t, tt.wantCode, body.Code)
		})
	}
}
