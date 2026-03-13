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

// TestDashboard_RDSData_Index covers the RDS Data dashboard index handler.
func TestDashboard_RDSData_Index(t *testing.T) {
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
			wantContains: "RDS Data",
		},
		{
			name: "renders executed statement",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, _, _, err := s.RDSDataHandler.Backend.ExecuteStatement(
					"arn:aws:rds:us-east-1:000000000000:cluster:test-cluster",
					"SELECT 42",
					"",
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "SELECT 42",
		},
		{
			name:         "shows empty state when no statements",
			wantCode:     http.StatusOK,
			wantContains: "No statements executed yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/rdsdata", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_RDSData_Index_NilOps covers rdsdataIndex when RDSDataOps is nil.
func TestDashboard_RDSData_Index_NilOps(t *testing.T) {
	t.Parallel()

	s := newStack(t)
	s.Dashboard.RDSDataOps = nil

	req := httptest.NewRequest(http.MethodGet, "/dashboard/rdsdata", nil)
	w := httptest.NewRecorder()
	serveHandler(s.Dashboard, w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
}

// TestDashboard_RDSData_Execute covers the rdsdataExecute handler.
func TestDashboard_RDSData_Execute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "execute statement redirects",
			formValues: map[string]string{
				"resource_arn": "arn:aws:rds:us-east-1:000000000000:cluster:test",
				"sql":          "SELECT 1",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing resource_arn returns bad request",
			formValues: map[string]string{"sql": "SELECT 1"},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "missing sql returns bad request",
			formValues: map[string]string{"resource_arn": "arn:aws:rds:us-east-1:000000000000:cluster:test"},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "empty form returns bad request",
			formValues: map[string]string{},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			for k, v := range tt.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/rdsdata/execute",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_RDSData_Execute_NilOps covers rdsdataExecute when RDSDataOps is nil.
func TestDashboard_RDSData_Execute_NilOps(t *testing.T) {
	t.Parallel()

	s := newStack(t)
	s.Dashboard.RDSDataOps = nil

	form := url.Values{
		"resource_arn": {"arn:aws:rds:us-east-1:000000000000:cluster:test"},
		"sql":          {"SELECT 1"},
	}
	req := httptest.NewRequest(http.MethodPost, "/dashboard/rdsdata/execute",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(s.Dashboard, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
