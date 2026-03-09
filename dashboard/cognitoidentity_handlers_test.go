package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDashboard_CognitoIdentity covers the Cognito Identity dashboard handler.
func TestDashboard_CognitoIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders when ops is nil",
			wantCode:     http.StatusOK,
			wantContains: "Cognito Identity Pools",
		},
		{
			name: "index renders pools",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.CognitoIdentityHandler.Backend.CreateIdentityPool(
					"my-test-pool",
					true,
					false,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "my-test-pool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/cognitoidentity", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}
