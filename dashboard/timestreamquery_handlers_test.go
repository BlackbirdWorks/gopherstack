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

// TestDashboard_TimestreamQuery_Index covers the Timestream Query dashboard index handler.
func TestDashboard_TimestreamQuery_Index(t *testing.T) {
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
			wantContains: "Timestream Query",
		},
		{
			name: "index shows created scheduled query",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TimestreamQueryHandler.Backend.CreateScheduledQuery(
					"test-query",
					"SELECT 1",
					"rate(1 hour)",
					"arn:aws:iam::000000000000:role/role",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "test-query",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No scheduled queries found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/timestreamquery", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_TimestreamQuery_Create covers the create scheduled query handler.
func TestDashboard_TimestreamQuery_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		queryName          string
		queryString        string
		scheduleExpression string
		wantCode           int
	}{
		{
			name:               "valid request redirects",
			queryName:          "my-query",
			queryString:        "SELECT 1",
			scheduleExpression: "rate(1 hour)",
			wantCode:           http.StatusSeeOther,
		},
		{
			name:               "missing name returns bad request",
			queryName:          "",
			queryString:        "SELECT 1",
			scheduleExpression: "rate(1 hour)",
			wantCode:           http.StatusBadRequest,
		},
		{
			name:               "missing query string returns bad request",
			queryName:          "my-query",
			queryString:        "",
			scheduleExpression: "rate(1 hour)",
			wantCode:           http.StatusBadRequest,
		},
		{
			name:               "missing schedule expression returns bad request",
			queryName:          "my-query",
			queryString:        "SELECT 1",
			scheduleExpression: "",
			wantCode:           http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			if tt.queryName != "" {
				form.Set("name", tt.queryName)
			}

			if tt.queryString != "" {
				form.Set("query_string", tt.queryString)
			}

			if tt.scheduleExpression != "" {
				form.Set("schedule_expression", tt.scheduleExpression)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/timestreamquery/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_TimestreamQuery_Delete covers the delete scheduled query handler.
func TestDashboard_TimestreamQuery_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		arn      string
		wantCode int
	}{
		{
			name: "valid arn redirects after delete",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				sq, err := s.TimestreamQueryHandler.Backend.CreateScheduledQuery(
					"del-query", "SELECT 1", "rate(1 hour)",
					"arn:aws:iam::000000000000:role/role",
					"", "", "", "", nil,
				)
				require.NoError(t, err)

				return sq.Arn
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "missing arn redirects to index",
			arn:      "",
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "nonexistent arn still redirects",
			arn:      "arn:aws:timestream:us-east-1:000000000000:scheduled-query/missing",
			wantCode: http.StatusSeeOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			arn := tt.arn
			if tt.setup != nil {
				arn = tt.setup(t, s)
			}

			form := url.Values{}
			if arn != "" {
				form.Set("arn", arn)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/timestreamquery/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
