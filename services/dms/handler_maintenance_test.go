package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

func TestHandler_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "maint-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				arn := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": arn,
					"ApplyAction":            "os-upgrade",
					"OptInType":              "immediate",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				rp, ok := resp["ResourcePendingMaintenanceActions"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, arn, rp["ResourceIdentifier"])
			},
		},
		{
			name: "missing_arn",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ApplyAction": "os-upgrade",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
					"ApplyAction":            "os-upgrade",
					"OptInType":              "immediate",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			// Locks gap #4 from PARITY.md: ApplyAction/OptInType previously
			// accepted any string. Real AWS documents a closed valid-values
			// list for both.
			name: "invalid_apply_action",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "maint-inst-2",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				arn := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": arn,
					"ApplyAction":            "bogus-action",
					"OptInType":              "immediate",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "invalid_opt_in_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "maint-inst-3",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				arn := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": arn,
					"ApplyAction":            "os-upgrade",
					"OptInType":              "bogus-opt-in",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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
