package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Organizations ---

func TestWorkMail_Organizations_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "create_returns_org_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "testcorp")
				assert.NotEmpty(t, orgID)
			},
		},
		{
			name: "describe_returns_expected_fields",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "mycompany")
				rec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				assert.Equal(t, orgID, m["OrganizationId"])
				assert.Equal(t, "mycompany", m["Alias"])
				assert.Equal(t, "ACTIVE", m["State"])
				assert.Contains(t, m["DefaultMailDomain"], "mycompany")
				assert.NotEmpty(t, m["ARN"])
				assert.NotEmpty(t, m["DirectoryId"])
				assert.NotEmpty(t, m["DirectoryType"])
				assert.NotZero(t, m["CompletedDate"])
			},
		},
		{
			name: "list_returns_organization",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				createTestOrg(t, h, "listtest")
				rec := doOp(t, h, "ListOrganizations", `{}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				summaries, ok := m["OrganizationSummaries"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, summaries)
				first := summaries[0].(map[string]any)
				assert.NotEmpty(t, first["OrganizationId"])
				assert.NotEmpty(t, first["Alias"])
				assert.NotEmpty(t, first["State"])
			},
		},
		{
			name: "delete_removes_organization",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "todelete")
				rec := doOp(t, h, "DeleteOrganization",
					fmt.Sprintf(`{"OrganizationId":%q,"DeleteDirectory":false}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				assert.Equal(t, orgID, m["OrganizationId"])
				assert.Equal(t, "DELETED", m["State"])
				// subsequent describe should 404
				rec2 := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "create_default_domain_visible_in_list_mail_domains",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domaintest")
				rec := doOp(t, h, "ListMailDomains", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				domains, ok := m["MailDomains"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, domains)
				d := domains[0].(map[string]any)
				assert.Contains(t, d["DomainName"].(string), "domaintest")
				assert.Equal(t, true, d["DefaultDomain"])
			},
		},
		{
			name: "duplicate_alias_returns_conflict",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				createTestOrg(t, h, "duporg")
				rec := doOp(t, h, "CreateOrganization", `{"Alias":"duporg"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				m := decodeJSON(t, rec)
				assert.Contains(t, m["__type"].(string), "AlreadyExists")
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

// TestDescribeOrganization_MigrationAdminField locks
// DescribeOrganizationOutput.MigrationAdmin's wire shape: the real API
// exposes no operation that ever sets it (no migration/interoperability
// admin flow is simulated), so the field must be present in the response
// struct (surviving a real client's field access) but always absent from
// the marshaled JSON via omitempty, matching every real organization that
// never configured migration.
func TestDescribeOrganization_MigrationAdminField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "migration-admin-org")

	rec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec.Code)

	m := decodeJSON(t, rec)
	_, present := m["MigrationAdmin"]
	assert.False(t, present, "MigrationAdmin should be omitted (empty) since nothing ever sets it")
}
