package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Email Monitoring Configuration ----

func TestEmailMonitoringConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		roleArn     string
		logGroupArn string
	}{
		{
			name:        "full config",
			roleArn:     "arn:aws:iam::000000000000:role/WorkMailMonitoringRole",
			logGroupArn: "arn:aws:logs:us-east-1:000000000000:log-group:/workmail/monitoring",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "monitoring-org")

			// Describe empty
			rec := doOp(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			assert.Empty(t, m["RoleArn"])

			// Put
			rec = doOp(t, h, "PutEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q,"RoleArn":%q,"LogGroupArn":%q}`,
				orgID, tc.roleArn, tc.logGroupArn,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe
			rec = doOp(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			assert.Equal(t, tc.roleArn, m["RoleArn"])
			assert.Equal(t, tc.logGroupArn, m["LogGroupArn"])

			// Delete
			rec = doOp(t, h, "DeleteEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe empty again
			rec = doOp(t, h, "DescribeEmailMonitoringConfiguration", fmt.Sprintf(
				`{"OrganizationId":%q}`, orgID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			assert.Empty(t, m["RoleArn"])
		})
	}
}
