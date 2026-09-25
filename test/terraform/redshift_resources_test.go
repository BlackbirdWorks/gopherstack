package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsvc "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_RedshiftResources provisions Redshift resources without prior
// terraform coverage: authentication profile, cluster IAM roles, cluster
// snapshot, HSM client certificate and configuration, event subscription,
// logging, scheduled action, snapshot copy (+ grant), snapshot schedule
// (+ association), usage limit, resource policy, partner, endpoint access
// and authorization, and a zero-ETL integration from an Aurora cluster.
func TestTerraform_RedshiftResources(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "redshift-resources",
			providerFn: megaRDSRedshiftProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Suffix": uuid.NewString()[:8]}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				suffix := vars["Suffix"].(string)
				client := createRedshiftClient(t)
				verifyRedshiftResourcesCluster(ctx, t, client, suffix)
				verifyRedshiftResourcesStandalone(ctx, t, client, suffix)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyRedshiftResourcesCluster(
	ctx context.Context,
	t *testing.T,
	client *redshiftsvc.Client,
	suffix string,
) {
	t.Helper()

	clusterID := "rdsh-cluster-" + suffix

	clOut, err := client.DescribeClusters(ctx, &redshiftsvc.DescribeClustersInput{
		ClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeClusters should succeed")
	require.Len(t, clOut.Clusters, 1)
	require.Len(t, clOut.Clusters[0].IamRoles, 1, "aws_redshift_cluster_iam_roles should attach one role")

	snapOut, err := client.DescribeClusterSnapshots(ctx, &redshiftsvc.DescribeClusterSnapshotsInput{
		SnapshotIdentifier: aws.String("rdsh-snapshot-" + suffix),
	})
	require.NoError(t, err, "DescribeClusterSnapshots should succeed")
	require.Len(t, snapOut.Snapshots, 1)

	subOut, err := client.DescribeEventSubscriptions(ctx, &redshiftsvc.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("rdsh-events-sub-" + suffix),
	})
	require.NoError(t, err, "DescribeEventSubscriptions should succeed")
	require.Len(t, subOut.EventSubscriptionsList, 1)

	limitOut, err := client.DescribeUsageLimits(ctx, &redshiftsvc.DescribeUsageLimitsInput{
		ClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeUsageLimits should succeed")
	require.Len(t, limitOut.UsageLimits, 1)

	epOut, err := client.DescribeEndpointAccess(ctx, &redshiftsvc.DescribeEndpointAccessInput{
		ClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeEndpointAccess should succeed")
	require.Len(t, epOut.EndpointAccessList, 1)

	authOut, err := client.DescribeEndpointAuthorization(ctx, &redshiftsvc.DescribeEndpointAuthorizationInput{
		ClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeEndpointAuthorization should succeed")
	require.Len(t, authOut.EndpointAuthorizationList, 1)
	assert.Equal(t, "111111111111", aws.ToString(authOut.EndpointAuthorizationList[0].Grantee))
}

func verifyRedshiftResourcesStandalone(
	ctx context.Context,
	t *testing.T,
	client *redshiftsvc.Client,
	suffix string,
) {
	t.Helper()

	profOut, err := client.DescribeAuthenticationProfiles(ctx, &redshiftsvc.DescribeAuthenticationProfilesInput{
		AuthenticationProfileName: aws.String("rdsh-auth-profile-" + suffix),
	})
	require.NoError(t, err, "DescribeAuthenticationProfiles should succeed")
	require.Len(t, profOut.AuthenticationProfiles, 1)

	hsmCertOut, err := client.DescribeHsmClientCertificates(ctx, &redshiftsvc.DescribeHsmClientCertificatesInput{
		HsmClientCertificateIdentifier: aws.String("rdsh-hsm-cert-" + suffix),
	})
	require.NoError(t, err, "DescribeHsmClientCertificates should succeed")
	require.Len(t, hsmCertOut.HsmClientCertificates, 1)

	hsmCfgOut, err := client.DescribeHsmConfigurations(ctx, &redshiftsvc.DescribeHsmConfigurationsInput{
		HsmConfigurationIdentifier: aws.String("rdsh-hsm-config-" + suffix),
	})
	require.NoError(t, err, "DescribeHsmConfigurations should succeed")
	require.Len(t, hsmCfgOut.HsmConfigurations, 1)

	grantOut, err := client.DescribeSnapshotCopyGrants(ctx, &redshiftsvc.DescribeSnapshotCopyGrantsInput{
		SnapshotCopyGrantName: aws.String("rdsh-copy-grant-" + suffix),
	})
	require.NoError(t, err, "DescribeSnapshotCopyGrants should succeed")
	require.Len(t, grantOut.SnapshotCopyGrants, 1)

	schedOut, err := client.DescribeSnapshotSchedules(ctx, &redshiftsvc.DescribeSnapshotSchedulesInput{
		ScheduleIdentifier: aws.String("rdsh-schedule-" + suffix),
	})
	require.NoError(t, err, "DescribeSnapshotSchedules should succeed")
	require.Len(t, schedOut.SnapshotSchedules, 1)
	require.Len(t, schedOut.SnapshotSchedules[0].AssociatedClusters, 1,
		"aws_redshift_snapshot_schedule_association should associate the cluster")

	actionOut, err := client.DescribeScheduledActions(ctx, &redshiftsvc.DescribeScheduledActionsInput{
		ScheduledActionName: aws.String("rdsh-scheduled-action-" + suffix),
	})
	require.NoError(t, err, "DescribeScheduledActions should succeed")
	require.Len(t, actionOut.ScheduledActions, 1)

	partnerOut, err := client.DescribePartners(ctx, &redshiftsvc.DescribePartnersInput{
		AccountId:         aws.String("000000000000"),
		ClusterIdentifier: aws.String("rdsh-cluster-" + suffix),
		DatabaseName:      aws.String("testdb"),
	})
	require.NoError(t, err, "DescribePartners should succeed")
	require.Len(t, partnerOut.PartnerIntegrationInfoList, 1)

	policyOut, err := client.GetResourcePolicy(ctx, &redshiftsvc.GetResourcePolicyInput{
		ResourceArn: aws.String(resourcePolicyArn(ctx, t, client, suffix)),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	require.NotNil(t, policyOut.ResourcePolicy)

	integOut, err := client.DescribeIntegrations(ctx, &redshiftsvc.DescribeIntegrationsInput{})
	require.NoError(t, err, "DescribeIntegrations should succeed")

	found := false
	for _, ig := range integOut.Integrations {
		if aws.ToString(ig.IntegrationName) == "rdsh-integration-"+suffix {
			found = true

			break
		}
	}
	assert.True(t, found, "rdsh-integration should be listed by DescribeIntegrations")
}

// resourcePolicyArn fetches the rdsh cluster snapshot's ARN so
// verifyRedshiftResourcesStandalone can look up the resource policy attached to it
// by aws_redshift_resource_policy.
func resourcePolicyArn(ctx context.Context, t *testing.T, client *redshiftsvc.Client, suffix string) string {
	t.Helper()

	out, err := client.DescribeClusterSnapshots(ctx, &redshiftsvc.DescribeClusterSnapshotsInput{
		SnapshotIdentifier: aws.String("rdsh-snapshot-" + suffix),
	})
	require.NoError(t, err, "DescribeClusterSnapshots should succeed")
	require.Len(t, out.Snapshots, 1)

	return aws.ToString(out.Snapshots[0].SnapshotArn)
}
