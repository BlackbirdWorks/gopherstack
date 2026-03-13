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

// TestDashboard_QLDB_Index covers the QLDB dashboard index handler.
func TestDashboard_QLDB_Index(t *testing.T) {
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
			wantContains: "QLDB Ledgers",
		},
		{
			name: "index renders created ledger",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.QLDBHandler.Backend.CreateLedger(
					"dashboard-test-ledger",
					"ALLOW_ALL",
					false,
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-ledger",
		},
		{
			name:         "index shows no ledgers message when empty",
			wantCode:     http.StatusOK,
			wantContains: "No ledgers found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/qldb", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_QLDB_Create covers the QLDB ledger create handler.
func TestDashboard_QLDB_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "create ledger redirects",
			formValues: map[string]string{
				"name":             "new-ledger",
				"permissions_mode": "ALLOW_ALL",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
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

			req := httptest.NewRequest(http.MethodPost, "/dashboard/qldb/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_QLDB_Delete covers the QLDB ledger delete handler.
func TestDashboard_QLDB_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		name       string
		ledgerName string
		wantCode   int
	}{
		{
			name: "delete existing ledger redirects",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.QLDBHandler.Backend.CreateLedger("del-ledger", "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			},
			ledgerName: "del-ledger",
			wantCode:   http.StatusSeeOther,
		},
		{
			name:       "missing ledger name returns bad request",
			ledgerName: "",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "not found ledger returns 404",
			ledgerName: "missing-ledger",
			wantCode:   http.StatusNotFound,
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
			if tt.ledgerName != "" {
				form.Set("name", tt.ledgerName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/qldb/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
