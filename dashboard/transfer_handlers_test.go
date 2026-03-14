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

// TestDashboard_Transfer_Index covers the Transfer dashboard index handler.
func TestDashboard_Transfer_Index(t *testing.T) {
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
			wantContains: "Transfer Servers",
		},
		{
			name: "index shows created server",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TransferHandler.Backend.CreateServer([]string{"SFTP"}, nil)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "SFTP",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No Transfer servers found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/transfer", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_Transfer_CreateServer covers the create server handler.
func TestDashboard_Transfer_CreateServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form     url.Values
		name     string
		wantCode int
	}{
		{
			name:     "create with SFTP",
			form:     url.Values{"protocol": {"SFTP"}},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "create with default protocol",
			form:     url.Values{},
			wantCode: http.StatusSeeOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/transfer/create-server",
				strings.NewReader(tt.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_Transfer_DeleteServer covers the delete server handler.
func TestDashboard_Transfer_DeleteServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		serverID string
		wantCode int
	}{
		{
			name: "delete existing server",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				server, err := s.TransferHandler.Backend.CreateServer([]string{"SFTP"}, nil)
				require.NoError(t, err)

				return server.ServerID
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "missing server_id",
			serverID: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent server",
			serverID: "s-doesnotexist12345",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			serverID := tt.serverID

			if tt.setup != nil {
				serverID = tt.setup(t, s)
			}

			form := url.Values{}
			if serverID != "" {
				form.Set("server_id", serverID)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/transfer/delete-server",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
