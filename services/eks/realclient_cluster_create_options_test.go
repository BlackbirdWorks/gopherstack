package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ClusterCreateOptions drives CreateCluster fields the
// reqfielddiff census flagged as dropped (gopherstack-xhu2t) through the
// real aws-sdk-go-v2 client: DeletionProtection, Logging, and UpgradePolicy
// previously weren't even decoded from the request body.
func TestRealClient_ClusterCreateOptions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "deletion_protection_blocks_delete", run: func(t *testing.T) {
			t.Helper()

			client := newTestEKSClient(t, newRealClientHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:    aws.String("dp-cluster"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
					SubnetIds: []string{"subnet-abc123"},
				},
				DeletionProtection: aws.Bool(true),
			})
			require.NoError(t, err)

			described, err := client.DescribeCluster(
				ctx,
				&ekssdk.DescribeClusterInput{Name: aws.String("dp-cluster")},
			)
			require.NoError(t, err)
			assert.True(t, aws.ToBool(described.Cluster.DeletionProtection))

			_, err = client.DeleteCluster(
				ctx,
				&ekssdk.DeleteClusterInput{Name: aws.String("dp-cluster")},
			)
			require.Error(
				t,
				err,
				"DeleteCluster must be rejected while deletion protection is enabled",
			)
		}},
		{name: "logging_applied_at_create", run: func(t *testing.T) {
			t.Helper()

			client := newTestEKSClient(t, newRealClientHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:    aws.String("log-cluster"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
					SubnetIds: []string{"subnet-abc123"},
				},
				Logging: &ekstypes.Logging{
					ClusterLogging: []ekstypes.LogSetup{
						{
							Types:   []ekstypes.LogType{ekstypes.LogTypeApi, ekstypes.LogTypeAudit},
							Enabled: aws.Bool(true),
						},
					},
				},
			})
			require.NoError(t, err)

			described, err := client.DescribeCluster(
				ctx,
				&ekssdk.DescribeClusterInput{Name: aws.String("log-cluster")},
			)
			require.NoError(t, err)
			require.NotNil(t, described.Cluster.Logging)
			require.Len(t, described.Cluster.Logging.ClusterLogging, 1)
			assert.True(t, aws.ToBool(described.Cluster.Logging.ClusterLogging[0].Enabled))
			assert.ElementsMatch(t, []ekstypes.LogType{ekstypes.LogTypeApi, ekstypes.LogTypeAudit},
				described.Cluster.Logging.ClusterLogging[0].Types)
		}},
		{name: "upgrade_policy_standard_overrides_default", run: func(t *testing.T) {
			t.Helper()

			client := newTestEKSClient(t, newRealClientHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:    aws.String("up-cluster"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
					SubnetIds: []string{"subnet-abc123"},
				},
				UpgradePolicy: &ekstypes.UpgradePolicyRequest{
					SupportType: ekstypes.SupportTypeStandard,
				},
			})
			require.NoError(t, err)

			described, err := client.DescribeCluster(
				ctx,
				&ekssdk.DescribeClusterInput{Name: aws.String("up-cluster")},
			)
			require.NoError(t, err)
			require.NotNil(t, described.Cluster.UpgradePolicy)
			assert.Equal(
				t,
				ekstypes.SupportTypeStandard,
				described.Cluster.UpgradePolicy.SupportType,
			)
		}},
		{name: "upgrade_policy_defaults_to_extended", run: func(t *testing.T) {
			t.Helper()

			client := newTestEKSClient(t, newRealClientHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:    aws.String("default-up-cluster"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
					SubnetIds: []string{"subnet-abc123"},
				},
			})
			require.NoError(t, err)

			described, err := client.DescribeCluster(ctx, &ekssdk.DescribeClusterInput{
				Name: aws.String("default-up-cluster"),
			})
			require.NoError(t, err)
			require.NotNil(t, described.Cluster.UpgradePolicy)
			assert.Equal(
				t,
				ekstypes.SupportTypeExtended,
				described.Cluster.UpgradePolicy.SupportType,
			)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_DescribeClusterVersionsDefaultOnly proves DefaultOnly
// filters the response to only default versions (previously ignored,
// always returning the full table).
func TestRealClient_DescribeClusterVersionsDefaultOnly(t *testing.T) {
	t.Parallel()

	client := newTestEKSClient(t, newRealClientHandler(t))
	ctx := t.Context()

	all, err := client.DescribeClusterVersions(ctx, &ekssdk.DescribeClusterVersionsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, all.ClusterVersions)

	defaultOnly, err := client.DescribeClusterVersions(ctx, &ekssdk.DescribeClusterVersionsInput{
		DefaultOnly: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotEmpty(t, defaultOnly.ClusterVersions)
	assert.Less(t, len(defaultOnly.ClusterVersions), len(all.ClusterVersions),
		"DefaultOnly must narrow the result set")

	for _, v := range defaultOnly.ClusterVersions {
		assert.True(
			t,
			v.DefaultVersion,
			"every returned version must be a default when DefaultOnly=true",
		)
	}
}

// TestRealClient_UpdateNodegroupVersionReleaseVersion proves ReleaseVersion
// is applied to the nodegroup (previously decoded nowhere, silently
// dropped).
func TestRealClient_UpdateNodegroupVersionReleaseVersion(t *testing.T) {
	t.Parallel()

	client := newTestEKSClient(t, newRealClientHandler(t))
	ctx := t.Context()
	createTestCluster(t, client, "rv-cluster")

	_, err := client.CreateNodegroup(ctx, &ekssdk.CreateNodegroupInput{
		ClusterName:   aws.String("rv-cluster"),
		NodegroupName: aws.String("ng-1"),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/node-role"),
		Subnets:       []string{"subnet-abc123"},
	})
	require.NoError(t, err)

	_, err = client.UpdateNodegroupVersion(ctx, &ekssdk.UpdateNodegroupVersionInput{
		ClusterName:    aws.String("rv-cluster"),
		NodegroupName:  aws.String("ng-1"),
		ReleaseVersion: aws.String("1.32.0-20250101"),
	})
	require.NoError(t, err)

	described, err := client.DescribeNodegroup(ctx, &ekssdk.DescribeNodegroupInput{
		ClusterName:   aws.String("rv-cluster"),
		NodegroupName: aws.String("ng-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.32.0-20250101", aws.ToString(described.Nodegroup.ReleaseVersion))
}
