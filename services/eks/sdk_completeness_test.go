package eks_test

import (
	"testing"

	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// eks client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend("000000000000", "us-east-1")
	h := eks.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ekssdk.Client{}, h.GetSupportedOperations(), []string{
		"AssociateAccessPolicy",
		"AssociateEncryptionConfig",
		"AssociateIdentityProviderConfig",
		"CreateAccessEntry",
		"CreateAddon",
		"CreateCapability",
		"CreateEksAnywhereSubscription",
		"CreateFargateProfile",
		"CreatePodIdentityAssociation",
		"DeleteAccessEntry",
		"DeleteAddon",
		"DeleteCapability",
		"DeleteEksAnywhereSubscription",
		"DeleteFargateProfile",
		"DeletePodIdentityAssociation",
		"DeregisterCluster",
		"DescribeAccessEntry",
		"DescribeAddon",
		"DescribeAddonConfiguration",
		"DescribeAddonVersions",
		"DescribeCapability",
		"DescribeClusterVersions",
		"DescribeEksAnywhereSubscription",
		"DescribeFargateProfile",
		"DescribeIdentityProviderConfig",
		"DescribeInsight",
		"DescribeInsightsRefresh",
		"DescribePodIdentityAssociation",
		"DescribeUpdate",
		"DisassociateAccessPolicy",
		"DisassociateIdentityProviderConfig",
		"ListAccessEntries",
		"ListAccessPolicies",
		"ListAddons",
		"ListAssociatedAccessPolicies",
		"ListCapabilities",
		"ListEksAnywhereSubscriptions",
		"ListFargateProfiles",
		"ListIdentityProviderConfigs",
		"ListInsights",
		"ListPodIdentityAssociations",
		"ListUpdates",
		"RegisterCluster",
		"StartInsightsRefresh",
		"UpdateAccessEntry",
		"UpdateAddon",
		"UpdateCapability",
		"UpdateClusterConfig",
		"UpdateClusterVersion",
		"UpdateEksAnywhereSubscription",
		"UpdateNodegroupVersion",
		"UpdatePodIdentityAssociation",
	})
}
