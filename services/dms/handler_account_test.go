package dms_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeAccountAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "returns_configured_account_id",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeAccountAttributes", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				assert.Equal(t, "123456789012", resp["UniqueAccountIdentifier"])
			},
		},
		{
			name: "returns_quota_list",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeAccountAttributes", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				quotas, ok := resp["AccountQuotas"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, quotas)
				// Verify expected quota names are present.
				quotaNames := make([]string, 0, len(quotas))
				for _, q := range quotas {
					quotaNames = append(quotaNames, q.(map[string]any)["AccountQuotaName"].(string))
				}
				assert.Contains(t, quotaNames, "ReplicationInstances")
				assert.Contains(t, quotaNames, "Endpoints")
			},
		},
		{
			name: "quota_used_reflects_resources",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "quota-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})

				rec := doDMS(t, h, "DescribeAccountAttributes", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				quotas := resp["AccountQuotas"].([]any)

				for _, q := range quotas {
					qm := q.(map[string]any)
					if qm["AccountQuotaName"] == "ReplicationInstances" {
						assert.InEpsilon(t, float64(1), qm["Used"], 0.01)

						return
					}
				}
				t.Fatal("ReplicationInstances quota not found")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
