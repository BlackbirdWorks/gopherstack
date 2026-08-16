package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
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
			// Locks the GetOrganizationStatistics wire-shape fix: real
			// GetOrganizationStatisticsOutput wraps everything under a
			// single organizationDetails object carrying updatedAt (epoch
			// seconds) alongside organizationStatistics -- both were
			// previously missing entirely.
			name: "org_statistics_wire_shape",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				resp := doJSON(t, h, http.MethodGet, "/organization/statistics", nil)

				details, ok := resp["organizationDetails"].(map[string]any)
				require.True(t, ok, "response must be wrapped under organizationDetails")

				updatedAt, ok := details["updatedAt"].(float64)
				require.True(t, ok, "organizationDetails.updatedAt must be a JSON number (epoch seconds)")
				assert.Positive(t, updatedAt)

				stats, ok := details["organizationStatistics"].(map[string]any)
				require.True(t, ok, "organizationDetails.organizationStatistics must be present")

				for _, key := range []string{
					"activeAccountsCount", "totalAccountsCount", "memberAccountsCount",
					"enabledAccountsCount", "countByFeature",
				} {
					assert.Containsf(t, stats, key, "organizationStatistics must include %s", key)
				}
			},
		},
		{
			// Locks that account counts reflect real member state, not the
			// unrelated orgAdminAccounts (delegated-administrator) table.
			name: "org_statistics_reflects_members",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				doRequest(t, h, http.MethodPost, "/detector/"+id+"/member", map[string]any{
					"accountDetails": []map[string]any{
						{"accountId": "222222222222", "email": "m1@example.com"},
					},
				})

				resp := doJSON(t, h, http.MethodGet, "/organization/statistics", nil)
				details := resp["organizationDetails"].(map[string]any)
				stats := details["organizationStatistics"].(map[string]any)

				// 1 member account created (relationship "Created", not yet
				// "Enabled") + this account itself.
				assert.InDelta(t, 2, stats["totalAccountsCount"], 0)
				assert.InDelta(t, 1, stats["memberAccountsCount"], 0)
				assert.InDelta(t, 0, stats["enabledAccountsCount"], 0)
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
