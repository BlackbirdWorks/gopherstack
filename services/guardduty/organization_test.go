package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "admin_account_lifecycle",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// EnableOrganizationAdminAccount
				rec := doRequest(t, h, http.MethodPost, "/admin/enable", map[string]any{
					"adminAccountId": "100000000001",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListOrganizationAdminAccounts
				rec = doRequest(t, h, http.MethodGet, "/admin", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				accounts, _ := listResp["adminAccounts"].([]any)
				assert.Len(t, accounts, 1)

				// DisableOrganizationAdminAccount
				rec = doRequest(t, h, http.MethodPost, "/admin/disable", map[string]any{
					"adminAccountId": "100000000001",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after disable
				rec = doRequest(t, h, http.MethodGet, "/admin", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				accounts2, _ := listResp["adminAccounts"].([]any)
				assert.Empty(t, accounts2)
			},
		},
		{
			name: "org_config",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				// DescribeOrganizationConfiguration
				rec := doRequest(t, h, http.MethodGet, "/detector/"+id+"/admin", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.NotNil(t, descResp["autoEnable"])

				// UpdateOrganizationConfiguration
				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/admin", map[string]any{
					"autoEnable": true,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/admin", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, true, descResp["autoEnable"])
			},
		},
		{
			name: "org_statistics",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/organization/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.fn(t, h)
		})
	}
}
