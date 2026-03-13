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

// TestDashboard_RedshiftData_Index covers the Redshift Data dashboard index handler.
func TestDashboard_RedshiftData_Index(t *testing.T) {
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
			wantContains: "Redshift Data Statements",
		},
		{
			name: "index renders executed statement",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.RedshiftDataHandler.Backend.ExecuteStatement(
					"SELECT 42", "dashboard-cluster", "", "dev", "", "", "",
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "SELECT 42",
		},
		{
			name:         "index shows empty state when no statements",
			wantCode:     http.StatusOK,
			wantContains: "No statements found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/redshiftdata", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_RedshiftData_Execute covers the Redshift Data execute handler.
func TestDashboard_RedshiftData_Execute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack)
		body     url.Values
		name     string
		wantCode int
	}{
		{
			name: "execute statement succeeds",
			body: url.Values{
				"sql":                []string{"SELECT 1"},
				"cluster_identifier": []string{"test-cluster"},
				"database":           []string{"dev"},
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "execute with empty sql returns bad request",
			body:     url.Values{},
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

			req := httptest.NewRequest(http.MethodPost, "/dashboard/redshiftdata/execute",
				strings.NewReader(tt.body.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_RedshiftData_Cancel covers the Redshift Data cancel handler.
func TestDashboard_RedshiftData_Cancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) url.Values
		name     string
		wantCode int
	}{
		{
			name: "cancel with empty id returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) url.Values {
				return url.Values{}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "cancel nonexistent statement returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) url.Values {
				return url.Values{"id": []string{"nonexistent-id"}}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)
			body := tt.setup(t, s)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/redshiftdata/cancel",
				strings.NewReader(body.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
