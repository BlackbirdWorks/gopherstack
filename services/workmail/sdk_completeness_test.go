package workmail_test

import (
	"testing"

	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// workmail client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)

	notImplemented := []string{
		"AssumeImpersonationRole",
		"CancelMailboxExportJob",
		"CreateAvailabilityConfiguration",
		"CreateIdentityCenterApplication",
		"CreateMobileDeviceAccessRule",
		"DeleteAvailabilityConfiguration",
		"DeleteEmailMonitoringConfiguration",
		"DeleteIdentityCenterApplication",
		"DeleteIdentityProviderConfiguration",
		"DeleteMobileDeviceAccessOverride",
		"DeleteMobileDeviceAccessRule",
		"DeletePersonalAccessToken",
		"DeleteRetentionPolicy",
		"DescribeEmailMonitoringConfiguration",
		"DescribeIdentityProviderConfiguration",
		"DescribeInboundDmarcSettings",
		"DescribeMailboxExportJob",
		"GetDefaultRetentionPolicy",
		"GetImpersonationRoleEffect",
		"GetMobileDeviceAccessEffect",
		"GetMobileDeviceAccessOverride",
		"GetPersonalAccessTokenMetadata",
		"ListAvailabilityConfigurations",
		"ListMailboxExportJobs",
		"ListMobileDeviceAccessOverrides",
		"ListMobileDeviceAccessRules",
		"ListPersonalAccessTokens",
		"PutEmailMonitoringConfiguration",
		"PutIdentityProviderConfiguration",
		"PutInboundDmarcSettings",
		"PutMobileDeviceAccessOverride",
		"PutRetentionPolicy",
		"StartMailboxExportJob",
		"TestAvailabilityConfiguration",
		"UpdateAvailabilityConfiguration",
		"UpdateMobileDeviceAccessRule",
	}

	sdkcheck.CheckCompleteness(t, &workmailsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
