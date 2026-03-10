package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDashboard_IoTDataPlane covers the IoT Data Plane dashboard handler.
func TestDashboard_IoTDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders page header",
			wantCode:     http.StatusOK,
			wantContains: "IoT Data Plane",
		},
		{
			name:         "index renders supported operations section",
			wantCode:     http.StatusOK,
			wantContains: "Supported Operations",
		},
		{
			name:         "index renders publish operation",
			wantCode:     http.StatusOK,
			wantContains: "Publish",
		},
		{
			name:         "index renders code snippet section",
			wantCode:     http.StatusOK,
			wantContains: "iot-data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			req := httptest.NewRequest(http.MethodGet, "/dashboard/iotdataplane", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}
