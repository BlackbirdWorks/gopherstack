package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// TestDashboard_SageMakerRuntime_Index covers the SageMaker Runtime dashboard index handler.
func TestDashboard_SageMakerRuntime_Index(t *testing.T) {
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
			wantContains: "SageMaker Runtime",
		},
		{
			name: "index renders recorded invocation",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				s.SageMakerRuntimeHandler.Backend.RecordInvocation(
					"InvokeEndpoint",
					"dashboard-test-endpoint",
					`{"data": "test"}`,
					`{"Body":"mock"}`,
				)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-endpoint",
		},
		{
			name:         "index shows empty state when no invocations",
			wantCode:     http.StatusOK,
			wantContains: "No endpoint invocations recorded yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/sagemakerrumtime", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}
