package sesv2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// Test_RouteMatrix_AgainstRealSDK checks sesv2.ParseSESv2Path (the same
// parser RouteMatcher/ExtractOperation/Handler use) against every
// currently-routed real operation path emitted by
// aws-sdk-go-v2/service/sesv2 v1.60.1's serializers -- one entry per real SDK
// operation, with {PathParam} placeholders substituted for sample values.
// This catches route-matcher bugs (wrong path segment, wrong HTTP method,
// wrong dispatch order) that a handler-level unit test calling the handler
// function directly would miss -- the exact bug class that made whole
// operation families unroutable in services/backup.
//
// Every real SDK route is now covered (the RPC-style tenant/resource-tenant
// paths, deliverability-dashboard sub-resources, insights, recommendations,
// reputation-entity listing, and the POST-based list-export-jobs/
// import-jobs/list variants were fixed in a later pass than the original
// route-matcher rewrite -- see services/sesv2/PARITY.md).
func Test_RouteMatrix_AgainstRealSDK(t *testing.T) {
	t.Parallel()

	cases := []struct {
		method string
		path   string
		wantOp string
	}{
		{
			method: "POST",
			path:   "/v2/email/metrics/batch",
			wantOp: "BatchGetMetricData",
		},
		{
			method: "PUT",
			path:   "/v2/email/export-jobs/job1/cancel",
			wantOp: "CancelExportJob",
		},
		{
			method: "POST",
			path:   "/v2/email/configuration-sets",
			wantOp: "CreateConfigurationSet",
		},
		{
			method: "POST",
			path:   "/v2/email/configuration-sets/cfgset1/event-destinations",
			wantOp: "CreateConfigurationSetEventDestination",
		},
		{
			method: "POST",
			path:   "/v2/email/contact-lists/list1/contacts",
			wantOp: "CreateContact",
		},
		{
			method: "POST",
			path:   "/v2/email/contact-lists",
			wantOp: "CreateContactList",
		},
		{
			method: "POST",
			path:   "/v2/email/custom-verification-email-templates",
			wantOp: "CreateCustomVerificationEmailTemplate",
		},
		{
			method: "POST",
			path:   "/v2/email/dedicated-ip-pools",
			wantOp: "CreateDedicatedIpPool",
		},
		{
			method: "POST",
			path:   "/v2/email/deliverability-dashboard/test",
			wantOp: "CreateDeliverabilityTestReport",
		},
		{
			method: "POST",
			path:   "/v2/email/identities",
			wantOp: "CreateEmailIdentity",
		},
		{
			method: "POST",
			path:   "/v2/email/identities/identity1/policies/policy1",
			wantOp: "CreateEmailIdentityPolicy",
		},
		{
			method: "POST",
			path:   "/v2/email/templates",
			wantOp: "CreateEmailTemplate",
		},
		{
			method: "POST",
			path:   "/v2/email/export-jobs",
			wantOp: "CreateExportJob",
		},
		{
			method: "POST",
			path:   "/v2/email/import-jobs",
			wantOp: "CreateImportJob",
		},
		{
			method: "POST",
			path:   "/v2/email/multi-region-endpoints",
			wantOp: "CreateMultiRegionEndpoint",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants",
			wantOp: "CreateTenant",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/resources",
			wantOp: "CreateTenantResourceAssociation",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/delete",
			wantOp: "DeleteTenant",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/resources/delete",
			wantOp: "DeleteTenantResourceAssociation",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/get",
			wantOp: "GetTenant",
		},
		{
			method: "POST",
			path:   "/v2/email/resources/tenants/list",
			wantOp: "ListResourceTenants",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/resources/list",
			wantOp: "ListTenantResources",
		},
		{
			method: "POST",
			path:   "/v2/email/tenants/list",
			wantOp: "ListTenants",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/test-reports/report1",
			wantOp: "GetDeliverabilityTestReport",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/test-reports",
			wantOp: "ListDeliverabilityTestReports",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/statistics-report/example.com",
			wantOp: "GetDomainStatisticsReport",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/campaigns/campaign1",
			wantOp: "GetDomainDeliverabilityCampaign",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/domains/example.com/campaigns",
			wantOp: "ListDomainDeliverabilityCampaigns",
		},
		{
			method: "POST",
			path:   "/v2/email/email-address-insights",
			wantOp: "GetEmailAddressInsights",
		},
		{
			method: "GET",
			path:   "/v2/email/insights/message1",
			wantOp: "GetMessageInsights",
		},
		{
			method: "POST",
			path:   "/v2/email/vdm/recommendations",
			wantOp: "ListRecommendations",
		},
		{
			method: "POST",
			path:   "/v2/email/reputation/entities",
			wantOp: "ListReputationEntities",
		},
		{
			method: "POST",
			path:   "/v2/email/list-export-jobs",
			wantOp: "ListExportJobs",
		},
		{
			method: "POST",
			path:   "/v2/email/import-jobs/list",
			wantOp: "ListImportJobs",
		},
		{
			method: "DELETE",
			path:   "/v2/email/configuration-sets/cfgset1",
			wantOp: "DeleteConfigurationSet",
		},
		{
			method: "DELETE",
			path:   "/v2/email/configuration-sets/cfgset1/event-destinations/dest1",
			wantOp: "DeleteConfigurationSetEventDestination",
		},
		{
			method: "DELETE",
			path:   "/v2/email/contact-lists/list1/contacts/a@example.com",
			wantOp: "DeleteContact",
		},
		{
			method: "DELETE",
			path:   "/v2/email/contact-lists/list1",
			wantOp: "DeleteContactList",
		},
		{
			method: "DELETE",
			path:   "/v2/email/custom-verification-email-templates/tmpl1",
			wantOp: "DeleteCustomVerificationEmailTemplate",
		},
		{
			method: "DELETE",
			path:   "/v2/email/dedicated-ip-pools/pool1",
			wantOp: "DeleteDedicatedIpPool",
		},
		{
			method: "DELETE",
			path:   "/v2/email/identities/identity1",
			wantOp: "DeleteEmailIdentity",
		},
		{
			method: "DELETE",
			path:   "/v2/email/identities/identity1/policies/policy1",
			wantOp: "DeleteEmailIdentityPolicy",
		},
		{
			method: "DELETE",
			path:   "/v2/email/templates/tmpl1",
			wantOp: "DeleteEmailTemplate",
		},
		{
			method: "DELETE",
			path:   "/v2/email/multi-region-endpoints/endpoint1",
			wantOp: "DeleteMultiRegionEndpoint",
		},
		{
			method: "DELETE",
			path:   "/v2/email/suppression/addresses/a@example.com",
			wantOp: "DeleteSuppressedDestination",
		},
		{
			method: "GET",
			path:   "/v2/email/account",
			wantOp: "GetAccount",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard/blacklist-report",
			wantOp: "GetBlacklistReports",
		},
		{
			method: "GET",
			path:   "/v2/email/configuration-sets/cfgset1",
			wantOp: "GetConfigurationSet",
		},
		{
			method: "GET",
			path:   "/v2/email/configuration-sets/cfgset1/event-destinations",
			wantOp: "GetConfigurationSetEventDestinations",
		},
		{
			method: "GET",
			path:   "/v2/email/contact-lists/list1/contacts/a@example.com",
			wantOp: "GetContact",
		},
		{
			method: "GET",
			path:   "/v2/email/contact-lists/list1",
			wantOp: "GetContactList",
		},
		{
			method: "GET",
			path:   "/v2/email/custom-verification-email-templates/tmpl1",
			wantOp: "GetCustomVerificationEmailTemplate",
		},
		{
			method: "GET",
			path:   "/v2/email/dedicated-ips/10.0.0.1",
			wantOp: "GetDedicatedIp",
		},
		{
			method: "GET",
			path:   "/v2/email/dedicated-ip-pools/pool1",
			wantOp: "GetDedicatedIpPool",
		},
		{
			method: "GET",
			path:   "/v2/email/dedicated-ips",
			wantOp: "GetDedicatedIps",
		},
		{
			method: "GET",
			path:   "/v2/email/deliverability-dashboard",
			wantOp: "GetDeliverabilityDashboardOptions",
		},
		{
			method: "GET",
			path:   "/v2/email/identities/identity1",
			wantOp: "GetEmailIdentity",
		},
		{
			method: "GET",
			path:   "/v2/email/identities/identity1/policies",
			wantOp: "GetEmailIdentityPolicies",
		},
		{
			method: "GET",
			path:   "/v2/email/templates/tmpl1",
			wantOp: "GetEmailTemplate",
		},
		{
			method: "GET",
			path:   "/v2/email/export-jobs/job1",
			wantOp: "GetExportJob",
		},
		{
			method: "GET",
			path:   "/v2/email/import-jobs/job1",
			wantOp: "GetImportJob",
		},
		{
			method: "GET",
			path:   "/v2/email/multi-region-endpoints/endpoint1",
			wantOp: "GetMultiRegionEndpoint",
		},
		{
			method: "GET",
			path:   "/v2/email/reputation/entities/CONFIGURATION_SET/ref1",
			wantOp: "GetReputationEntity",
		},
		{
			method: "GET",
			path:   "/v2/email/suppression/addresses/a@example.com",
			wantOp: "GetSuppressedDestination",
		},
		{
			method: "GET",
			path:   "/v2/email/configuration-sets",
			wantOp: "ListConfigurationSets",
		},
		{
			method: "GET",
			path:   "/v2/email/contact-lists",
			wantOp: "ListContactLists",
		},
		{
			method: "POST",
			path:   "/v2/email/contact-lists/list1/contacts/list",
			wantOp: "ListContacts",
		},
		{
			method: "GET",
			path:   "/v2/email/custom-verification-email-templates",
			wantOp: "ListCustomVerificationEmailTemplates",
		},
		{
			method: "GET",
			path:   "/v2/email/dedicated-ip-pools",
			wantOp: "ListDedicatedIpPools",
		},
		{
			method: "GET",
			path:   "/v2/email/identities",
			wantOp: "ListEmailIdentities",
		},
		{
			method: "GET",
			path:   "/v2/email/templates",
			wantOp: "ListEmailTemplates",
		},
		{
			method: "GET",
			path:   "/v2/email/multi-region-endpoints",
			wantOp: "ListMultiRegionEndpoints",
		},
		{
			method: "GET",
			path:   "/v2/email/suppression/addresses",
			wantOp: "ListSuppressedDestinations",
		},
		{
			method: "GET",
			path:   "/v2/email/tags",
			wantOp: "ListTagsForResource",
		},
		{
			method: "PUT",
			path:   "/v2/email/account/dedicated-ips/warmup",
			wantOp: "PutAccountDedicatedIpWarmupAttributes",
		},
		{
			method: "POST",
			path:   "/v2/email/account/details",
			wantOp: "PutAccountDetails",
		},
		{
			method: "PUT",
			path:   "/v2/email/account/sending",
			wantOp: "PutAccountSendingAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/account/suppression",
			wantOp: "PutAccountSuppressionAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/account/vdm",
			wantOp: "PutAccountVdmAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/archiving-options",
			wantOp: "PutConfigurationSetArchivingOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/delivery-options",
			wantOp: "PutConfigurationSetDeliveryOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/reputation-options",
			wantOp: "PutConfigurationSetReputationOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/sending",
			wantOp: "PutConfigurationSetSendingOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/suppression-options",
			wantOp: "PutConfigurationSetSuppressionOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/tracking-options",
			wantOp: "PutConfigurationSetTrackingOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/vdm-options",
			wantOp: "PutConfigurationSetVdmOptions",
		},
		{
			method: "PUT",
			path:   "/v2/email/dedicated-ips/10.0.0.1/pool",
			wantOp: "PutDedicatedIpInPool",
		},
		{
			method: "PUT",
			path:   "/v2/email/dedicated-ip-pools/pool1/scaling",
			wantOp: "PutDedicatedIpPoolScalingAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/dedicated-ips/10.0.0.1/warmup",
			wantOp: "PutDedicatedIpWarmupAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/deliverability-dashboard",
			wantOp: "PutDeliverabilityDashboardOption",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/configuration-set",
			wantOp: "PutEmailIdentityConfigurationSetAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/dkim",
			wantOp: "PutEmailIdentityDkimAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/dkim/signing",
			wantOp: "PutEmailIdentityDkimSigningAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/feedback",
			wantOp: "PutEmailIdentityFeedbackAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/mail-from",
			wantOp: "PutEmailIdentityMailFromAttributes",
		},
		{
			method: "PUT",
			path:   "/v2/email/suppression/addresses",
			wantOp: "PutSuppressedDestination",
		},
		{
			method: "POST",
			path:   "/v2/email/outbound-bulk-emails",
			wantOp: "SendBulkEmail",
		},
		{
			method: "POST",
			path:   "/v2/email/outbound-custom-verification-emails",
			wantOp: "SendCustomVerificationEmail",
		},
		{
			method: "POST",
			path:   "/v2/email/outbound-emails",
			wantOp: "SendEmail",
		},
		{
			method: "POST",
			path:   "/v2/email/tags",
			wantOp: "TagResource",
		},
		{
			method: "POST",
			path:   "/v2/email/templates/tmpl1/render",
			wantOp: "TestRenderEmailTemplate",
		},
		{
			method: "DELETE",
			path:   "/v2/email/tags",
			wantOp: "UntagResource",
		},
		{
			method: "PUT",
			path:   "/v2/email/configuration-sets/cfgset1/event-destinations/dest1",
			wantOp: "UpdateConfigurationSetEventDestination",
		},
		{
			method: "PUT",
			path:   "/v2/email/contact-lists/list1/contacts/a@example.com",
			wantOp: "UpdateContact",
		},
		{
			method: "PUT",
			path:   "/v2/email/contact-lists/list1",
			wantOp: "UpdateContactList",
		},
		{
			method: "PUT",
			path:   "/v2/email/custom-verification-email-templates/tmpl1",
			wantOp: "UpdateCustomVerificationEmailTemplate",
		},
		{
			method: "PUT",
			path:   "/v2/email/identities/identity1/policies/policy1",
			wantOp: "UpdateEmailIdentityPolicy",
		},
		{
			method: "PUT",
			path:   "/v2/email/templates/tmpl1",
			wantOp: "UpdateEmailTemplate",
		},
		{
			method: "PUT",
			path:   "/v2/email/reputation/entities/CONFIGURATION_SET/ref1/customer-managed-status",
			wantOp: "UpdateReputationEntityCustomerManagedStatus",
		},
		{
			method: "PUT",
			path:   "/v2/email/reputation/entities/CONFIGURATION_SET/ref1/policy",
			wantOp: "UpdateReputationEntityPolicy",
		},
	}

	for _, tc := range cases {
		t.Run(tc.method+"_"+tc.path, func(t *testing.T) {
			t.Parallel()

			gotOp, _ := sesv2.ParseSESv2Path(tc.method, tc.path)
			if gotOp != tc.wantOp {
				t.Errorf("ParseSESv2Path(%q, %q) = %q, want %q", tc.method, tc.path, gotOp, tc.wantOp)
			}
		})
	}
}
