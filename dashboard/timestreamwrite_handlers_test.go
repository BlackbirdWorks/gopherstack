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

// TestDashboard_TimestreamWrite_Index covers the Timestream Write dashboard index handler.
func TestDashboard_TimestreamWrite_Index(t *testing.T) {
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
			wantContains: "Timestream Write",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No databases found",
		},
		{
			name: "index shows database",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TimestreamWriteHandler.Backend.CreateDatabase("test-db")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "test-db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/timestreamwrite", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_TimestreamWrite_CreateDatabase covers the create database handler.
func TestDashboard_TimestreamWrite_CreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dbName   string
		wantCode int
	}{
		{
			name:     "valid request redirects",
			dbName:   "new-db",
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "missing name returns bad request",
			dbName:   "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			if tt.dbName != "" {
				form.Set("name", tt.dbName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/timestreamwrite/create-database",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_TimestreamWrite_DeleteDatabase covers the delete database handler.
func TestDashboard_TimestreamWrite_DeleteDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack)
		name     string
		dbName   string
		wantCode int
	}{
		{
			name:   "valid request redirects",
			dbName: "del-db",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TimestreamWriteHandler.Backend.CreateDatabase("del-db")
				require.NoError(t, err)
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "non-existent database returns bad request",
			dbName:   "missing-db",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing name returns bad request",
			dbName:   "",
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

			form := url.Values{}
			if tt.dbName != "" {
				form.Set("name", tt.dbName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/timestreamwrite/delete-database",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
