package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// TestDashboard_QLDBSession_Index covers the QLDB Session dashboard index handler.
func TestDashboard_QLDBSession_Index(t *testing.T) {
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
			wantContains: "QLDB Session",
		},
		{
			name: "index renders active session",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.QLDBSessionHandler.Backend.StartSession("dashboard-test-ledger")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-ledger",
		},
		{
			name:         "index shows no sessions message when empty",
			wantCode:     http.StatusOK,
			wantContains: "No active sessions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/qldbsession", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}
