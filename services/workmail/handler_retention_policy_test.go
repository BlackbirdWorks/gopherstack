package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Retention Policies ----

func TestRetentionPolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policyName   string
		description  string
		folderName   string
		folderAction string
		folderPeriod int
	}{
		{
			name:         "inbox delete after 365 days",
			policyName:   "default-retention",
			description:  "Default 365 day retention",
			folderName:   "INBOX",
			folderAction: "DELETE",
			folderPeriod: 365,
		},
		{
			name:         "sent items permanently delete",
			policyName:   "sent-retention",
			description:  "Sent items policy",
			folderName:   "SENT_ITEMS",
			folderAction: "PERMANENTLY_DELETE",
			folderPeriod: 730,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "retention-org")

			// Get before put → not found
			rec := doOp(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			// Put
			rec = doOp(t, h, "PutRetentionPolicy", fmt.Sprintf(
				`{"OrganizationId":%q,"Name":%q,"Description":%q,"FolderConfigurations":[{"Name":%q,"Action":%q,"Period":%d}]}`,
				orgID,
				tc.policyName,
				tc.description,
				tc.folderName,
				tc.folderAction,
				tc.folderPeriod,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get
			rec = doOp(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m := decodeJSON(t, rec)
			assert.Equal(t, tc.policyName, m["Name"])
			assert.Equal(t, tc.description, m["Description"])
			folderCfgs := m["FolderConfigurations"].([]any)
			require.Len(t, folderCfgs, 1)
			fc := folderCfgs[0].(map[string]any)
			assert.Equal(t, tc.folderName, fc["Name"])
			assert.Equal(t, tc.folderAction, fc["Action"])

			policyID := m["Id"].(string)

			// Delete
			rec = doOp(t, h, "DeleteRetentionPolicy", fmt.Sprintf(
				`{"OrganizationId":%q,"Id":%q}`, orgID, policyID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify gone
			rec = doOp(t, h, "GetDefaultRetentionPolicy", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
