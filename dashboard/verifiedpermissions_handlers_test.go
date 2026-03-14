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

// TestDashboard_VerifiedPermissions_Index covers the Verified Permissions dashboard index handler.
func TestDashboard_VerifiedPermissions_Index(t *testing.T) {
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
			wantContains: "Verified Permissions",
		},
		{
			name: "index shows created policy store",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.VerifiedPermissionsHandler.Backend.CreatePolicyStore("My Store", nil)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "My Store",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No policy stores found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/verifiedpermissions", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_VerifiedPermissions_CreatePolicyStore covers the create policy store handler.
func TestDashboard_VerifiedPermissions_CreatePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form     url.Values
		name     string
		wantCode int
	}{
		{
			name:     "create with description",
			form:     url.Values{"description": {"My test store"}},
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "create without description",
			form:     url.Values{},
			wantCode: http.StatusSeeOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/verifiedpermissions/create-policy-store",
				strings.NewReader(tt.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_VerifiedPermissions_DeletePolicyStore covers the delete policy store handler.
func TestDashboard_VerifiedPermissions_DeletePolicyStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*testing.T, *teststack.Stack) string
		name          string
		policyStoreID string
		wantCode      int
	}{
		{
			name: "delete existing policy store",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				ps, err := s.VerifiedPermissionsHandler.Backend.CreatePolicyStore("test", nil)
				require.NoError(t, err)

				return ps.PolicyStoreID
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:          "missing policy_store_id",
			policyStoreID: "",
			wantCode:      http.StatusBadRequest,
		},
		{
			name:          "non-existent policy store",
			policyStoreID: "nonexistent-id",
			wantCode:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			policyStoreID := tt.policyStoreID

			if tt.setup != nil {
				policyStoreID = tt.setup(t, s)
			}

			form := url.Values{}
			if policyStoreID != "" {
				form.Set("policy_store_id", policyStoreID)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/verifiedpermissions/delete-policy-store",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
