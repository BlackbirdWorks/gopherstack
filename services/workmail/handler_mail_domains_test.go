package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Mail Domains ---

func TestWorkMail_MailDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "register_and_get_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg1")
				rec := doOp(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "GetMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := decodeJSON(t, rec2)
				assert.Equal(t, "example.com", m["DomainName"])
				assert.Equal(t, false, m["IsDefault"])
				assert.NotEmpty(t, m["OwnershipVerificationStatus"])
			},
		},
		{
			name: "deregister_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg2")
				doOp(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"toremove.com"}`, orgID,
				))
				rec := doOp(t, h, "DeregisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"toremove.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "deregister_default_domain_fails",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg3")
				// get the default domain name
				rec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				m := decodeJSON(t, rec)
				defaultDomain := m["DefaultMailDomain"].(string)
				rec2 := doOp(t, h, "DeregisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":%q}`, orgID, defaultDomain,
				))
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "update_default_mail_domain",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg4")
				doOp(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				rec := doOp(t, h, "UpdateDefaultMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "GetMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"newdefault.com"}`, orgID,
				))
				m := decodeJSON(t, rec2)
				assert.Equal(t, true, m["IsDefault"])
			},
		},
		{
			name: "list_mail_domains",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "domainorg5")
				doOp(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":"extra.com"}`, orgID,
				))
				rec := doOp(t, h, "ListMailDomains", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				domains, ok := m["MailDomains"].([]any)
				require.True(t, ok)
				// default + extra
				assert.GreaterOrEqual(t, len(domains), 2)
				d := domains[0].(map[string]any)
				assert.NotEmpty(t, d["DomainName"])
				_, hasDefault := d["DefaultDomain"]
				assert.True(t, hasDefault)
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

// TestGetMailDomain_DkimAndRecords locks GetMailDomainOutput.
// DkimVerificationStatus and .Records (the recommended DNS record list):
// previously the backend modeled neither -- DkimVerificationStatus was
// absent from the wire shape entirely and Records, though present on the
// MailDomain struct, was never populated by RegisterMailDomain/
// CreateOrganization.
func TestGetMailDomain_DkimAndRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *workmail.Handler, orgID string) string
		name       string
		wantStatus string
	}{
		{
			name: "registered_domain_is_pending",
			setup: func(t *testing.T, h *workmail.Handler, orgID string) string {
				t.Helper()
				domain := "dkim-registered.example"
				require.Equal(t, http.StatusOK, doOp(t, h, "RegisterMailDomain", fmt.Sprintf(
					`{"OrganizationId":%q,"DomainName":%q}`, orgID, domain,
				)).Code)

				return domain
			},
			wantStatus: "PENDING",
		},
		{
			name: "default_domain_is_verified",
			setup: func(t *testing.T, h *workmail.Handler, orgID string) string {
				t.Helper()
				rec := doOp(t, h, "DescribeOrganization", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
				require.Equal(t, http.StatusOK, rec.Code)

				return decodeJSON(t, rec)["DefaultMailDomain"].(string)
			},
			wantStatus: "VERIFIED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			orgID := createTestOrg(t, h, "dkim-org-"+tc.name)
			domain := tc.setup(t, h, orgID)

			rec := doOp(t, h, "GetMailDomain", fmt.Sprintf(
				`{"OrganizationId":%q,"DomainName":%q}`, orgID, domain,
			))
			require.Equal(t, http.StatusOK, rec.Code)

			m := decodeJSON(t, rec)
			assert.Equal(t, tc.wantStatus, m["DkimVerificationStatus"])

			records, ok := m["Records"].([]any)
			require.True(t, ok, "Records field missing or wrong type")
			require.NotEmpty(t, records)
			for _, raw := range records {
				rec := raw.(map[string]any)
				assert.NotEmpty(t, rec["Hostname"])
				assert.NotEmpty(t, rec["Type"])
				assert.NotEmpty(t, rec["Value"])
			}
			// MX + SPF TXT + autodiscover CNAME + 3 DKIM CNAMEs.
			assert.Len(t, records, 6)
		})
	}
}
