package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CreateClusterConfigMembers(t *testing.T) {
	t.Parallel()

	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/abcd"

	tests := []struct {
		mutate  func(in *ekssdk.CreateClusterInput)
		check   func(t *testing.T, c *ekstypes.Cluster)
		name    string
		wantErr string
	}{
		{
			name: "members_round_trip",
			mutate: func(in *ekssdk.CreateClusterInput) {
				in.EncryptionConfig = []ekstypes.EncryptionConfig{{
					Provider: &ekstypes.Provider{KeyArn: aws.String(keyARN)},
				}}
				in.ZonalShiftConfig = &ekstypes.ZonalShiftConfigRequest{Enabled: aws.Bool(true)}
				in.ControlPlaneScalingConfig = &ekstypes.ControlPlaneScalingConfig{
					Tier: ekstypes.ProvisionedControlPlaneTierTierXl,
				}
				in.KubeApiServerConfig = &ekstypes.KubeApiServerConfigRequest{EventTtl: aws.String("30m")}
				in.RemoteNetworkConfig = &ekstypes.RemoteNetworkConfigRequest{
					RemoteNodeNetworks: []ekstypes.RemoteNodeNetwork{{Cidrs: []string{"10.50.0.0/16"}}},
				}
			},
			check: func(t *testing.T, c *ekstypes.Cluster) {
				t.Helper()
				require.Len(t, c.EncryptionConfig, 1)
				assert.Equal(t, keyARN, aws.ToString(c.EncryptionConfig[0].Provider.KeyArn))
				assert.True(t, aws.ToBool(c.ZonalShiftConfig.Enabled))
				assert.Equal(t, ekstypes.ProvisionedControlPlaneTierTierXl, c.ControlPlaneScalingConfig.Tier)
				assert.Equal(t, "30m", aws.ToString(c.KubeApiServerConfig.EventTtl))
				require.Len(t, c.RemoteNetworkConfig.RemoteNodeNetworks, 1)
				assert.Equal(t, []string{"10.50.0.0/16"}, c.RemoteNetworkConfig.RemoteNodeNetworks[0].Cidrs)
			},
		},
		{
			name: "invalid_scaling_tier",
			mutate: func(in *ekssdk.CreateClusterInput) {
				in.ControlPlaneScalingConfig = &ekstypes.ControlPlaneScalingConfig{Tier: "huge"}
			},
			wantErr: "InvalidParameterException",
		},
		{
			name: "invalid_kms_key",
			mutate: func(in *ekssdk.CreateClusterInput) {
				in.EncryptionConfig = []ekstypes.EncryptionConfig{{
					Provider: &ekstypes.Provider{KeyArn: aws.String("not-an-arn")},
				}}
			},
			wantErr: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			in := &ekssdk.CreateClusterInput{
				Name:               aws.String("cfg-members"),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
			}
			tt.mutate(in)

			_, err := client.CreateCluster(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeCluster(
				t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("cfg-members")},
			)
			require.NoError(t, err)
			tt.check(t, got.Cluster)
		})
	}
}

func TestRealClient_UpdateClusterConfigMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(in *ekssdk.UpdateClusterConfigInput)
		check      func(t *testing.T, c *ekstypes.Cluster)
		name       string
		wantErr    string
		wantParams []ekstypes.UpdateParamType
	}{
		{
			name: "deletion_protection_upgrade_policy_elb",
			mutate: func(in *ekssdk.UpdateClusterConfigInput) {
				in.DeletionProtection = aws.Bool(true)
				in.UpgradePolicy = &ekstypes.UpgradePolicyRequest{SupportType: ekstypes.SupportTypeStandard}
				in.KubernetesNetworkConfig = &ekstypes.KubernetesNetworkConfigRequest{
					ElasticLoadBalancing: &ekstypes.ElasticLoadBalancing{Enabled: aws.Bool(true)},
				}
			},
			check: func(t *testing.T, c *ekstypes.Cluster) {
				t.Helper()
				assert.True(t, aws.ToBool(c.DeletionProtection))
				assert.Equal(t, ekstypes.SupportTypeStandard, c.UpgradePolicy.SupportType)
				assert.True(t, aws.ToBool(c.KubernetesNetworkConfig.ElasticLoadBalancing.Enabled))
				assert.Equal(t, ekstypes.IpFamilyIpv4, c.KubernetesNetworkConfig.IpFamily)
			},
			wantParams: []ekstypes.UpdateParamType{
				ekstypes.UpdateParamTypeDeletionProtection,
				ekstypes.UpdateParamTypeUpgradePolicy,
				ekstypes.UpdateParamTypeKubernetesNetworkConfig,
			},
		},
		{
			name: "blocks_stored",
			mutate: func(in *ekssdk.UpdateClusterConfigInput) {
				in.ZonalShiftConfig = &ekstypes.ZonalShiftConfigRequest{Enabled: aws.Bool(true)}
				in.ControlPlaneScalingConfig = &ekstypes.ControlPlaneScalingConfig{
					Tier: ekstypes.ProvisionedControlPlaneTierTier2xl,
				}
			},
			check: func(t *testing.T, c *ekstypes.Cluster) {
				t.Helper()
				assert.True(t, aws.ToBool(c.ZonalShiftConfig.Enabled))
				assert.Equal(t, ekstypes.ProvisionedControlPlaneTierTier2xl, c.ControlPlaneScalingConfig.Tier)
			},
			wantParams: []ekstypes.UpdateParamType{
				ekstypes.UpdateParamTypePreviousTier,
				ekstypes.UpdateParamTypeUpdatedTier,
				ekstypes.UpdateParamTypeZonalShiftConfig,
			},
		},
		{
			name: "invalid_tier",
			mutate: func(in *ekssdk.UpdateClusterConfigInput) {
				in.ControlPlaneScalingConfig = &ekstypes.ControlPlaneScalingConfig{Tier: "huge"}
			},
			wantErr: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			createTestCluster(t, client, "upd-members")

			in := &ekssdk.UpdateClusterConfigInput{Name: aws.String("upd-members")}
			tt.mutate(in)

			out, err := client.UpdateClusterConfig(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			var types []ekstypes.UpdateParamType
			for _, p := range out.Update.Params {
				types = append(types, p.Type)
			}

			assert.ElementsMatch(t, tt.wantParams, types)

			got, err := client.DescribeCluster(
				t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("upd-members")},
			)
			require.NoError(t, err)
			tt.check(t, got.Cluster)
		})
	}
}

func TestRealClient_UpdateNodegroupVersionLaunchTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		createLT *ekstypes.LaunchTemplateSpecification
		updateLT *ekstypes.LaunchTemplateSpecification
		wantErr  string
		wantVer  string
	}{
		{
			name:     "version_applied",
			createLT: &ekstypes.LaunchTemplateSpecification{Name: aws.String("lt-a"), Version: aws.String("1")},
			updateLT: &ekstypes.LaunchTemplateSpecification{Name: aws.String("lt-a"), Version: aws.String("2")},
			wantVer:  "2",
		},
		{
			name:     "different_name_rejected",
			createLT: &ekstypes.LaunchTemplateSpecification{Name: aws.String("lt-a"), Version: aws.String("1")},
			updateLT: &ekstypes.LaunchTemplateSpecification{Name: aws.String("lt-b"), Version: aws.String("2")},
			wantErr:  "InvalidParameterException",
		},
		{
			name:     "none_at_create_rejected",
			updateLT: &ekstypes.LaunchTemplateSpecification{Name: aws.String("lt-a"), Version: aws.String("2")},
			wantErr:  "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			createTestCluster(t, client, "lt-cluster")

			_, err := client.CreateNodegroup(t.Context(), &ekssdk.CreateNodegroupInput{
				ClusterName:    aws.String("lt-cluster"),
				NodegroupName:  aws.String("ng"),
				NodeRole:       aws.String("arn:aws:iam::123456789012:role/node-role"),
				Subnets:        []string{"subnet-abc123"},
				LaunchTemplate: tt.createLT,
			})
			require.NoError(t, err)

			_, err = client.UpdateNodegroupVersion(t.Context(), &ekssdk.UpdateNodegroupVersionInput{
				ClusterName:    aws.String("lt-cluster"),
				NodegroupName:  aws.String("ng"),
				LaunchTemplate: tt.updateLT,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeNodegroup(t.Context(), &ekssdk.DescribeNodegroupInput{
				ClusterName: aws.String("lt-cluster"), NodegroupName: aws.String("ng"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantVer, aws.ToString(got.Nodegroup.LaunchTemplate.Version))
		})
	}
}

func TestRealClient_PodIdentityTargetRoleArn(t *testing.T) {
	t.Parallel()

	const (
		target  = "arn:aws:iam::210987654321:role/target"
		updated = "arn:aws:iam::210987654321:role/target-2"
	)

	client := newTestEKSClient(t, newRealClientHandler(t))
	createTestCluster(t, client, "pi-cluster")

	created, err := client.CreatePodIdentityAssociation(t.Context(), &ekssdk.CreatePodIdentityAssociationInput{
		ClusterName:    aws.String("pi-cluster"),
		Namespace:      aws.String("ns"),
		ServiceAccount: aws.String("sa"),
		RoleArn:        aws.String("arn:aws:iam::123456789012:role/pod"),
		TargetRoleArn:  aws.String(target),
	})
	require.NoError(t, err)
	assert.Equal(t, target, aws.ToString(created.Association.TargetRoleArn))

	upd, err := client.UpdatePodIdentityAssociation(t.Context(), &ekssdk.UpdatePodIdentityAssociationInput{
		ClusterName:   aws.String("pi-cluster"),
		AssociationId: created.Association.AssociationId,
		TargetRoleArn: aws.String(updated),
	})
	require.NoError(t, err)
	assert.Equal(t, updated, aws.ToString(upd.Association.TargetRoleArn))

	got, err := client.DescribePodIdentityAssociation(t.Context(), &ekssdk.DescribePodIdentityAssociationInput{
		ClusterName: aws.String("pi-cluster"), AssociationId: created.Association.AssociationId,
	})
	require.NoError(t, err)
	assert.Equal(t, updated, aws.ToString(got.Association.TargetRoleArn))
}

func TestRealClient_DescribeVersionFilters(t *testing.T) {
	t.Parallel()

	client := newTestEKSClient(t, newRealClientHandler(t))

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "cluster_versions_list", run: func(t *testing.T) {
			t.Helper()

			out, err := client.DescribeClusterVersions(t.Context(), &ekssdk.DescribeClusterVersionsInput{
				ClusterVersions: []string{"1.31", "1.29"},
			})
			require.NoError(t, err)
			require.Len(t, out.ClusterVersions, 2)
		}},
		{name: "cluster_type_mismatch", run: func(t *testing.T) {
			t.Helper()

			out, err := client.DescribeClusterVersions(t.Context(), &ekssdk.DescribeClusterVersionsInput{
				ClusterType: aws.String("eks-anywhere"),
			})
			require.NoError(t, err)
			assert.Empty(t, out.ClusterVersions)
		}},
		{name: "version_status", run: func(t *testing.T) {
			t.Helper()

			out, err := client.DescribeClusterVersions(t.Context(), &ekssdk.DescribeClusterVersionsInput{
				VersionStatus: ekstypes.VersionStatusUnsupported,
			})
			require.NoError(t, err)

			for _, v := range out.ClusterVersions {
				assert.Equal(t, ekstypes.VersionStatusUnsupported, v.VersionStatus)
			}
		}},
		{name: "addon_owners", run: func(t *testing.T) {
			t.Helper()

			none, err := client.DescribeAddonVersions(t.Context(), &ekssdk.DescribeAddonVersionsInput{
				Owners: []string{"aws-marketplace"},
			})
			require.NoError(t, err)
			assert.Empty(t, none.Addons)

			some, err := client.DescribeAddonVersions(t.Context(), &ekssdk.DescribeAddonVersionsInput{
				Owners: []string{"aws"}, Publishers: []string{"eks"},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, some.Addons)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
