package sesv2_test

import (
	"testing"

	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// sesv2 client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &sesv2sdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateExportJob",
		"CreateImportJob",
		"CreateMultiRegionEndpoint",
		"CreateTenant",
		"CreateTenantResourceAssociation",
		"DeleteConfigurationSetEventDestination",
		"DeleteContact",
		"DeleteContactList",
		"DeleteCustomVerificationEmailTemplate",
		"DeleteDedicatedIpPool",
		"DeleteEmailIdentityPolicy",
		"DeleteEmailTemplate",
		"DeleteMultiRegionEndpoint",
		"DeleteSuppressedDestination",
		"DeleteTenant",
		"DeleteTenantResourceAssociation",
		"GetAccount",
		"GetBlacklistReports",
		"GetConfigurationSetEventDestinations",
		"GetContact",
		"GetContactList",
		"GetCustomVerificationEmailTemplate",
		"GetDedicatedIp",
		"GetDedicatedIpPool",
		"GetDedicatedIps",
		"GetDeliverabilityDashboardOptions",
		"GetDeliverabilityTestReport",
		"GetDomainDeliverabilityCampaign",
		"GetDomainStatisticsReport",
		"GetEmailAddressInsights",
		"GetEmailIdentityPolicies",
		"GetEmailTemplate",
		"GetExportJob",
		"GetImportJob",
		"GetMessageInsights",
		"GetMultiRegionEndpoint",
		"GetReputationEntity",
		"GetSuppressedDestination",
		"GetTenant",
		"ListContactLists",
		"ListContacts",
		"ListCustomVerificationEmailTemplates",
		"ListDedicatedIpPools",
		"ListDeliverabilityTestReports",
		"ListDomainDeliverabilityCampaigns",
		"ListEmailTemplates",
		"ListExportJobs",
		"ListImportJobs",
		"ListMultiRegionEndpoints",
		"ListRecommendations",
		"ListReputationEntities",
		"ListResourceTenants",
		"ListSuppressedDestinations",
		"ListTenantResources",
		"ListTenants",
		"PutAccountDedicatedIpWarmupAttributes",
		"PutAccountDetails",
		"PutAccountSendingAttributes",
		"PutAccountSuppressionAttributes",
		"PutAccountVdmAttributes",
		"PutConfigurationSetArchivingOptions",
		"PutConfigurationSetDeliveryOptions",
		"PutConfigurationSetReputationOptions",
		"PutConfigurationSetSendingOptions",
		"PutConfigurationSetSuppressionOptions",
		"PutConfigurationSetTrackingOptions",
		"PutConfigurationSetVdmOptions",
		"PutDedicatedIpInPool",
		"PutDedicatedIpPoolScalingAttributes",
		"PutDedicatedIpWarmupAttributes",
		"PutDeliverabilityDashboardOption",
		"PutEmailIdentityConfigurationSetAttributes",
		"PutEmailIdentityDkimAttributes",
		"PutEmailIdentityDkimSigningAttributes",
		"PutEmailIdentityFeedbackAttributes",
		"PutEmailIdentityMailFromAttributes",
		"PutSuppressedDestination",
		"SendBulkEmail",
		"SendCustomVerificationEmail",
		"TestRenderEmailTemplate",
		"UpdateConfigurationSetEventDestination",
		"UpdateContact",
		"UpdateContactList",
		"UpdateCustomVerificationEmailTemplate",
		"UpdateEmailIdentityPolicy",
		"UpdateEmailTemplate",
		"UpdateReputationEntityCustomerManagedStatus",
		"UpdateReputationEntityPolicy",
	})
}
