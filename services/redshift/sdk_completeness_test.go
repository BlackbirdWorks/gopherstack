package redshift_test

import (
	"testing"

	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// redshift client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &redshiftsdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateAuthenticationProfile",
		"CreateCustomDomainAssociation",
		"CreateEndpointAccess",
		"CreateHsmClientCertificate",
		"CreateHsmConfiguration",
		"CreateIntegration",
		"CreateRedshiftIdcApplication",
		"CreateScheduledAction",
		"CreateSnapshotCopyGrant",
		"CreateSnapshotSchedule",
		"CreateUsageLimit",
		"DeauthorizeDataShare",
		"DeleteAuthenticationProfile",
		"DeleteCustomDomainAssociation",
		"DeleteEndpointAccess",
		"DeleteHsmClientCertificate",
		"DeleteHsmConfiguration",
		"DeleteIntegration",
		"DeletePartner",
		"DeleteRedshiftIdcApplication",
		"DeleteResourcePolicy",
		"DeleteScheduledAction",
		"DeleteSnapshotCopyGrant",
		"DeleteSnapshotSchedule",
		"DeleteUsageLimit",
		"DeregisterNamespace",
		"DescribeAccountAttributes",
		"DescribeAuthenticationProfiles",
		"DescribeClusterDbRevisions",
		"DescribeClusterTracks",
		"DescribeClusterVersions",
		"DescribeCustomDomainAssociations",
		"DescribeDataShares",
		"DescribeDataSharesForConsumer",
		"DescribeDataSharesForProducer",
		"DescribeEndpointAccess",
		"DescribeEndpointAuthorization",
		"DescribeHsmClientCertificates",
		"DescribeHsmConfigurations",
		"DescribeInboundIntegrations",
		"DescribeIntegrations",
		"DescribeNodeConfigurationOptions",
		"DescribeOrderableClusterOptions",
		"DescribePartners",
		"DescribeRedshiftIdcApplications",
		"DescribeResize",
		"DescribeScheduledActions",
		"DescribeSnapshotCopyGrants",
		"DescribeSnapshotSchedules",
		"DescribeStorage",
		"DescribeTableRestoreStatus",
		"DescribeUsageLimits",
		"DisableSnapshotCopy",
		"DisassociateDataShareConsumer",
		"EnableSnapshotCopy",
		"FailoverPrimaryCompute",
		"GetClusterCredentials",
		"GetClusterCredentialsWithIAM",
		"GetIdentityCenterAuthToken",
		"GetResourcePolicy",
		"ListRecommendations",
		"ModifyAquaConfiguration",
		"ModifyAuthenticationProfile",
		"ModifyClusterDbRevision",
		"ModifyClusterSnapshot",
		"ModifyClusterSnapshotSchedule",
		"ModifyCustomDomainAssociation",
		"ModifyEndpointAccess",
		"ModifyIntegration",
		"ModifyLakehouseConfiguration",
		"ModifyRedshiftIdcApplication",
		"ModifyScheduledAction",
		"ModifySnapshotCopyRetentionPeriod",
		"ModifySnapshotSchedule",
		"ModifyUsageLimit",
		"PutResourcePolicy",
		"RegisterNamespace",
		"RejectDataShare",
		"RestoreTableFromClusterSnapshot",
		"RevokeEndpointAccess",
		"RevokeSnapshotAccess",
		"UpdatePartnerStatus",
	})
}
