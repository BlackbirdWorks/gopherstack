package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrgAdmin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "enable_list_disable",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// EnableOrganizationAdminAccount
				rec := doRequest(t, h, http.MethodPost, "/admin", map[string]any{
					"adminAccountId": "111111111111",
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
				rec = doRequest(t, h, http.MethodDelete, "/admin?adminAccountId=111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify removed
				rec = doRequest(t, h, http.MethodGet, "/admin", nil)
				var afterList map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterList))
				accountsAfter, _ := afterList["adminAccounts"].([]any)
				assert.Empty(t, accountsAfter)
			},
		},
		{
			name: "describe_and_update_org_configuration",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// DescribeOrganizationConfiguration
				rec := doRequest(t, h, http.MethodGet, "/admin/configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				autoEnable, _ := descResp["autoEnable"].(bool)
				assert.False(t, autoEnable)

				// UpdateOrganizationConfiguration
				rec = doRequest(t, h, http.MethodPatch, "/admin/configuration", map[string]any{
					"autoEnable": true,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify change
				rec = doRequest(t, h, http.MethodGet, "/admin/configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.True(t, updated["autoEnable"].(bool))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
