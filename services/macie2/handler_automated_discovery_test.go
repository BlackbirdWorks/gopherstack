package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestAutomatedDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_update_configuration",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetAutomatedDiscoveryConfiguration
				rec := doRequest(t, h, http.MethodGet, "/automated-discovery/configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var cfg map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
				assert.Equal(t, "DISABLED", cfg["status"])

				// UpdateAutomatedDiscoveryConfiguration
				rec = doRequest(t, h, http.MethodPut, "/automated-discovery/configuration", map[string]any{
					"status":                        "ENABLED",
					"autoEnableOrganizationMembers": "NEW",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/automated-discovery/configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "ENABLED", updated["status"])
				assert.Equal(t, "NEW", updated["autoEnableOrganizationMembers"])
			},
		},
		{
			name: "list_and_batch_update_accounts",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// BatchUpdateAutomatedDiscoveryAccounts
				rec := doRequest(t, h, http.MethodPatch, "/automated-discovery/accounts", map[string]any{
					"accounts": []map[string]any{
						{"accountId": "111111111111", "status": "ENABLED"},
						{"accountId": "222222222222", "status": "DISABLED"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListAutomatedDiscoveryAccounts
				rec = doRequest(t, h, http.MethodGet, "/automated-discovery/accounts", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				items, _ := listResp["items"].([]any)
				assert.Len(t, items, 2)
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
