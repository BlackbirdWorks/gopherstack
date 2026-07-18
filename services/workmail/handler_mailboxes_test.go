package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Mailbox Permissions ---

func TestWorkMail_MailboxPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "put_and_list_permissions",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "permorg")
				ownerID := createTestUser(t, h, orgID, "owner", "Owner")
				granteeID := createTestUser(t, h, orgID, "grantee", "Grantee")
				rec := doOp(t, h, "PutMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q,"PermissionValues":["FULL_ACCESS"]}`,
					orgID, ownerID, granteeID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, ownerID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := decodeJSON(t, rec2)
				perms, ok := m["Permissions"].([]any)
				require.True(t, ok)
				require.Len(t, perms, 1)
				p := perms[0].(map[string]any)
				assert.Equal(t, granteeID, p["GranteeId"])
				assert.NotEmpty(t, p["GranteeType"])
				pvs, ok := p["PermissionValues"].([]any)
				require.True(t, ok)
				assert.Contains(t, pvs, "FULL_ACCESS")
			},
		},
		{
			name: "delete_permissions",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "delpermorg")
				ownerID := createTestUser(t, h, orgID, "delpowner", "Del Owner")
				granteeID := createTestUser(t, h, orgID, "delpgrantee", "Del Grantee")
				doOp(t, h, "PutMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q,"PermissionValues":["SEND_AS"]}`,
					orgID, ownerID, granteeID,
				))
				rec := doOp(t, h, "DeleteMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q,"GranteeId":%q}`, orgID, ownerID, granteeID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListMailboxPermissions", fmt.Sprintf(
					`{"OrganizationId":%q,"EntityId":%q}`, orgID, ownerID,
				))
				m := decodeJSON(t, rec2)
				perms := m["Permissions"].([]any)
				assert.Empty(t, perms)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.run(t, h)
		})
	}
}

// ---- Mailbox Export Jobs ----

func TestMailboxExportJobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		description  string
		s3BucketName string
		s3Prefix     string
	}{
		{
			name:         "basic export",
			description:  "Full mailbox export",
			s3BucketName: "my-export-bucket",
			s3Prefix:     "exports/2024",
		},
		{
			name:         "no description",
			description:  "",
			s3BucketName: "another-bucket",
			s3Prefix:     "prefix/subdir",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "export-org")
			userID := createTestUser(t, h, orgID, "exportuser", "Export User")

			// Start
			rec := doOp(t, h, "StartMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"EntityId":%q,"Description":%q,"RoleArn":"arn:aws:iam::000:role/ExportRole","KmsKeyArn":"arn:aws:kms:us-east-1:000:key/abc","S3BucketName":%q,"S3Prefix":%q}`, //nolint:lll // existing issue.
				orgID,
				userID,
				tc.description,
				tc.s3BucketName,
				tc.s3Prefix,
			))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			m := decodeJSON(t, rec)
			jobID, ok := m["JobId"].(string)
			require.True(t, ok)
			require.NotEmpty(t, jobID)

			// Describe
			rec = doOp(t, h, "DescribeMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q}`, orgID, jobID,
			))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			assert.Equal(t, "RUNNING", m["State"])
			assert.Equal(t, tc.s3BucketName, m["S3BucketName"])
			assert.Equal(t, tc.s3Prefix, m["S3Prefix"])

			// List
			rec = doOp(t, h, "ListMailboxExportJobs", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
			require.Equal(t, http.StatusOK, rec.Code)
			m = decodeJSON(t, rec)
			jobs := m["Jobs"].([]any)
			assert.Len(t, jobs, 1)
			assert.Equal(t, jobID, jobs[0].(map[string]any)["JobId"])

			// Cancel
			rec = doOp(t, h, "CancelMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q,"ClientToken":"tok-123"}`, orgID, jobID,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe after cancel
			rec = doOp(t, h, "DescribeMailboxExportJob", fmt.Sprintf(
				`{"OrganizationId":%q,"JobId":%q}`, orgID, jobID,
			))
			m = decodeJSON(t, rec)
			assert.Equal(t, "CANCELLED", m["State"])
		})
	}
}
