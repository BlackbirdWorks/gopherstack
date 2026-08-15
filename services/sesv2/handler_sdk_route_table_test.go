package sesv2_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real SES v2
// operation, extracted from sesv2@v1.66.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- parseSESv2Path (handler_routes.go) does not
// validate ID shape, so the literal value doesn't matter here, only that the
// path matches Op.
//
// A systematic check for a shared method+path-without-query template across
// all 112 ops found zero collisions, so no *required dynamic* (non-template)
// member -- the s3/glacier vacuity-trap class -- was needed to disambiguate
// any route in this table.
//
// This service has a second failure mode beyond an unrecognized path: an op
// that parseSESv2Path resolves to one of the opXxx constants but that
// dispatchOp (handler.go) has no case for. All 112 ops were confirmed wired
// across dispatchCoreOps/dispatchNewOps/dispatchExtendedOps
// (handler_dispatch.go) before writing this table, so that branch should be
// unreachable here -- see TestExtractOperation_SDKRouteTable's second
// assertion, which checks for it anyway since Handler() is what actually
// proves it, not a handler_dispatch.go headcount.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchGetMetricData", "POST", "/v2/email/metrics/batch"},
		{"CancelExportJob", "PUT", "/v2/email/export-jobs/PLACEHOLDER/cancel"},
		{"CreateConfigurationSet", "POST", "/v2/email/configuration-sets"},
		{
			"CreateConfigurationSetEventDestination",
			"POST",
			"/v2/email/configuration-sets/PLACEHOLDER/event-destinations",
		},
		{"CreateContact", "POST", "/v2/email/contact-lists/PLACEHOLDER/contacts"},
		{"CreateContactList", "POST", "/v2/email/contact-lists"},
		{"CreateCustomVerificationEmailTemplate", "POST", "/v2/email/custom-verification-email-templates"},
		{"CreateDedicatedIpPool", "POST", "/v2/email/dedicated-ip-pools"},
		{"CreateDeliverabilityTestReport", "POST", "/v2/email/deliverability-dashboard/test"},
		{"CreateEmailIdentity", "POST", "/v2/email/identities"},
		{"CreateEmailIdentityPolicy", "POST", "/v2/email/identities/PLACEHOLDER/policies/PLACEHOLDER"},
		{"CreateEmailTemplate", "POST", "/v2/email/templates"},
		{"CreateExportJob", "POST", "/v2/email/export-jobs"},
		{"CreateImportJob", "POST", "/v2/email/import-jobs"},
		{"CreateMultiRegionEndpoint", "POST", "/v2/email/multi-region-endpoints"},
		{"CreateTenant", "POST", "/v2/email/tenants"},
		{"CreateTenantResourceAssociation", "POST", "/v2/email/tenants/resources"},
		{"DeleteConfigurationSet", "DELETE", "/v2/email/configuration-sets/PLACEHOLDER"},
		{
			"DeleteConfigurationSetEventDestination",
			"DELETE",
			"/v2/email/configuration-sets/PLACEHOLDER/event-destinations/PLACEHOLDER",
		},
		{"DeleteContact", "DELETE", "/v2/email/contact-lists/PLACEHOLDER/contacts/PLACEHOLDER"},
		{"DeleteContactList", "DELETE", "/v2/email/contact-lists/PLACEHOLDER"},
		{
			"DeleteCustomVerificationEmailTemplate",
			"DELETE",
			"/v2/email/custom-verification-email-templates/PLACEHOLDER",
		},
		{"DeleteDedicatedIpPool", "DELETE", "/v2/email/dedicated-ip-pools/PLACEHOLDER"},
		{"DeleteEmailIdentity", "DELETE", "/v2/email/identities/PLACEHOLDER"},
		{"DeleteEmailIdentityPolicy", "DELETE", "/v2/email/identities/PLACEHOLDER/policies/PLACEHOLDER"},
		{"DeleteEmailTemplate", "DELETE", "/v2/email/templates/PLACEHOLDER"},
		{"DeleteMultiRegionEndpoint", "DELETE", "/v2/email/multi-region-endpoints/PLACEHOLDER"},
		{"DeleteSuppressedDestination", "DELETE", "/v2/email/suppression/addresses/PLACEHOLDER"},
		{"DeleteTenant", "POST", "/v2/email/tenants/delete"},
		{"DeleteTenantResourceAssociation", "POST", "/v2/email/tenants/resources/delete"},
		{"GetAccount", "GET", "/v2/email/account"},
		{"GetBlacklistReports", "GET", "/v2/email/deliverability-dashboard/blacklist-report"},
		{"GetConfigurationSet", "GET", "/v2/email/configuration-sets/PLACEHOLDER"},
		{"GetConfigurationSetEventDestinations", "GET", "/v2/email/configuration-sets/PLACEHOLDER/event-destinations"},
		{"GetContact", "GET", "/v2/email/contact-lists/PLACEHOLDER/contacts/PLACEHOLDER"},
		{"GetContactList", "GET", "/v2/email/contact-lists/PLACEHOLDER"},
		{"GetCustomVerificationEmailTemplate", "GET", "/v2/email/custom-verification-email-templates/PLACEHOLDER"},
		{"GetDedicatedIp", "GET", "/v2/email/dedicated-ips/PLACEHOLDER"},
		{"GetDedicatedIpPool", "GET", "/v2/email/dedicated-ip-pools/PLACEHOLDER"},
		{"GetDedicatedIps", "GET", "/v2/email/dedicated-ips"},
		{"GetDeliverabilityDashboardOptions", "GET", "/v2/email/deliverability-dashboard"},
		{"GetDeliverabilityTestReport", "GET", "/v2/email/deliverability-dashboard/test-reports/PLACEHOLDER"},
		{"GetDomainDeliverabilityCampaign", "GET", "/v2/email/deliverability-dashboard/campaigns/PLACEHOLDER"},
		{"GetDomainStatisticsReport", "GET", "/v2/email/deliverability-dashboard/statistics-report/PLACEHOLDER"},
		{"GetEmailAddressInsights", "POST", "/v2/email/email-address-insights"},
		{"GetEmailIdentity", "GET", "/v2/email/identities/PLACEHOLDER"},
		{"GetEmailIdentityPolicies", "GET", "/v2/email/identities/PLACEHOLDER/policies"},
		{"GetEmailTemplate", "GET", "/v2/email/templates/PLACEHOLDER"},
		{"GetExportJob", "GET", "/v2/email/export-jobs/PLACEHOLDER"},
		{"GetImportJob", "GET", "/v2/email/import-jobs/PLACEHOLDER"},
		{"GetMessageInsights", "GET", "/v2/email/insights/PLACEHOLDER"},
		{"GetMultiRegionEndpoint", "GET", "/v2/email/multi-region-endpoints/PLACEHOLDER"},
		{"GetReputationEntity", "GET", "/v2/email/reputation/entities/PLACEHOLDER/PLACEHOLDER"},
		{"GetSuppressedDestination", "GET", "/v2/email/suppression/addresses/PLACEHOLDER"},
		{"GetTenant", "POST", "/v2/email/tenants/get"},
		{"ListConfigurationSets", "GET", "/v2/email/configuration-sets"},
		{"ListContactLists", "GET", "/v2/email/contact-lists"},
		{"ListContacts", "POST", "/v2/email/contact-lists/PLACEHOLDER/contacts/list"},
		{"ListCustomVerificationEmailTemplates", "GET", "/v2/email/custom-verification-email-templates"},
		{"ListDedicatedIpPools", "GET", "/v2/email/dedicated-ip-pools"},
		{"ListDeliverabilityTestReports", "GET", "/v2/email/deliverability-dashboard/test-reports"},
		{
			"ListDomainDeliverabilityCampaigns",
			"GET",
			"/v2/email/deliverability-dashboard/domains/PLACEHOLDER/campaigns",
		},
		{"ListEmailIdentities", "GET", "/v2/email/identities"},
		{"ListEmailTemplates", "GET", "/v2/email/templates"},
		{"ListExportJobs", "POST", "/v2/email/list-export-jobs"},
		{"ListImportJobs", "POST", "/v2/email/import-jobs/list"},
		{"ListMultiRegionEndpoints", "GET", "/v2/email/multi-region-endpoints"},
		{"ListRecommendations", "POST", "/v2/email/vdm/recommendations"},
		{"ListReputationEntities", "POST", "/v2/email/reputation/entities"},
		{"ListResourceTenants", "POST", "/v2/email/resources/tenants/list"},
		{"ListSuppressedDestinations", "GET", "/v2/email/suppression/addresses"},
		{"ListTagsForResource", "GET", "/v2/email/tags"},
		{"ListTenantResources", "POST", "/v2/email/tenants/resources/list"},
		{"ListTenants", "POST", "/v2/email/tenants/list"},
		{"PutAccountDedicatedIpWarmupAttributes", "PUT", "/v2/email/account/dedicated-ips/warmup"},
		{"PutAccountDetails", "POST", "/v2/email/account/details"},
		{"PutAccountPricingAttributes", "PUT", "/v2/email/account/pricing-attributes"},
		{"PutAccountSendingAttributes", "PUT", "/v2/email/account/sending"},
		{"PutAccountSuppressionAttributes", "PUT", "/v2/email/account/suppression"},
		{"PutAccountVdmAttributes", "PUT", "/v2/email/account/vdm"},
		{"PutConfigurationSetArchivingOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/archiving-options"},
		{"PutConfigurationSetDeliveryOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/delivery-options"},
		{"PutConfigurationSetReputationOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/reputation-options"},
		{"PutConfigurationSetSendingOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/sending"},
		{
			"PutConfigurationSetSuppressionOptions",
			"PUT",
			"/v2/email/configuration-sets/PLACEHOLDER/suppression-options",
		},
		{"PutConfigurationSetTrackingOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/tracking-options"},
		{"PutConfigurationSetVdmOptions", "PUT", "/v2/email/configuration-sets/PLACEHOLDER/vdm-options"},
		{"PutDedicatedIpInPool", "PUT", "/v2/email/dedicated-ips/PLACEHOLDER/pool"},
		{"PutDedicatedIpPoolScalingAttributes", "PUT", "/v2/email/dedicated-ip-pools/PLACEHOLDER/scaling"},
		{"PutDedicatedIpWarmupAttributes", "PUT", "/v2/email/dedicated-ips/PLACEHOLDER/warmup"},
		{"PutDeliverabilityDashboardOption", "PUT", "/v2/email/deliverability-dashboard"},
		{"PutEmailIdentityConfigurationSetAttributes", "PUT", "/v2/email/identities/PLACEHOLDER/configuration-set"},
		{"PutEmailIdentityDkimAttributes", "PUT", "/v2/email/identities/PLACEHOLDER/dkim"},
		{"PutEmailIdentityDkimSigningAttributes", "PUT", "/v2/email/identities/PLACEHOLDER/dkim/signing"},
		{"PutEmailIdentityFeedbackAttributes", "PUT", "/v2/email/identities/PLACEHOLDER/feedback"},
		{"PutEmailIdentityMailFromAttributes", "PUT", "/v2/email/identities/PLACEHOLDER/mail-from"},
		{"PutSuppressedDestination", "PUT", "/v2/email/suppression/addresses"},
		{"PutTenantSuppressionAttributes", "POST", "/v2/email/tenant/suppression"},
		{"SendBulkEmail", "POST", "/v2/email/outbound-bulk-emails"},
		{"SendCustomVerificationEmail", "POST", "/v2/email/outbound-custom-verification-emails"},
		{"SendEmail", "POST", "/v2/email/outbound-emails"},
		{"TagResource", "POST", "/v2/email/tags"},
		{"TestRenderEmailTemplate", "POST", "/v2/email/templates/PLACEHOLDER/render"},
		{"UntagResource", "DELETE", "/v2/email/tags"},
		{
			"UpdateConfigurationSetEventDestination",
			"PUT",
			"/v2/email/configuration-sets/PLACEHOLDER/event-destinations/PLACEHOLDER",
		},
		{"UpdateContact", "PUT", "/v2/email/contact-lists/PLACEHOLDER/contacts/PLACEHOLDER"},
		{"UpdateContactList", "PUT", "/v2/email/contact-lists/PLACEHOLDER"},
		{"UpdateCustomVerificationEmailTemplate", "PUT", "/v2/email/custom-verification-email-templates/PLACEHOLDER"},
		{"UpdateEmailIdentityPolicy", "PUT", "/v2/email/identities/PLACEHOLDER/policies/PLACEHOLDER"},
		{"UpdateEmailTemplate", "PUT", "/v2/email/templates/PLACEHOLDER"},
		{
			"UpdateReputationEntityCustomerManagedStatus",
			"PUT",
			"/v2/email/reputation/entities/PLACEHOLDER/PLACEHOLDER/customer-managed-status",
		},
		{"UpdateReputationEntityPolicy", "PUT", "/v2/email/reputation/entities/PLACEHOLDER/PLACEHOLDER/policy"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SES v2 op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseSESv2Path resolves it to the right op, all 112 ops against
// sesv2's real op count. It then drives the same request through the real
// Handler() and checks both of its distinct failure sentinels: the exact
// "no route for" prefix Handler() (handler.go) emits when op == unknownAction
// (a path parseSESv2Path never resolved), and the exact "is not a valid SES
// v2 operation" suffix dispatchOp emits when op is a real opXxx constant that
// none of dispatchCoreOps/dispatchNewOps/dispatchExtendedOps claims. Both
// phrases are confirmed to appear nowhere else in non-test source (grepped
// directly), so neither can false-positive against a domain error message.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "no route for",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
			assert.NotContains(t, rec.Body.String(), "is not a valid SES v2 operation",
				"method=%s path=%s op=%s: op recognized but not wired to a dispatcher", tc.method, tc.path, tc.op)
		})
	}
}
