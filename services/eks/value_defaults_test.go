package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCluster_CreateAppliesDocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		vpc        *types.VpcConfigRequest
		name       string
		wantPublic bool
	}{
		{name: "omitted public access", vpc: &types.VpcConfigRequest{SubnetIds: []string{"s1"}}, wantPublic: true},
		{
			name: "explicit private only",
			vpc:  &types.VpcConfigRequest{SubnetIds: []string{"s1"}, EndpointPublicAccess: aws.Bool(false)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerAndBackend(t)
			c := newTestEKSClient(t, h)

			_, err := c.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
				Name: aws.String(
					"c",
				),
				RoleArn:            aws.String("arn:aws:iam::000000000000:role/r"),
				ResourcesVpcConfig: tc.vpc,
			})
			require.NoError(t, err)

			got, err := c.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("c")})
			require.NoError(t, err)
			assert.Equal(t, tc.wantPublic, got.Cluster.ResourcesVpcConfig.EndpointPublicAccess)
			assert.Equal(t, types.IpFamilyIpv4, got.Cluster.KubernetesNetworkConfig.IpFamily)

			if tc.wantPublic {
				assert.Equal(t, []string{"0.0.0.0/0"}, got.Cluster.ResourcesVpcConfig.PublicAccessCidrs)
			}
		})
	}
}

func TestNodegroup_CreateAppliesDocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		scaling       *types.NodegroupScalingConfig
		name          string
		wantInstances []string
		wantDisk      int32
		wantMin       int32
		wantMax       int32
		wantDesired   int32
	}{
		{
			name: "all omitted", wantInstances: []string{"t3.medium"}, wantDisk: 20,
			wantMin: 1, wantMax: 2, wantDesired: 2,
		},
		{
			name: "explicit scaling kept",
			scaling: &types.NodegroupScalingConfig{
				MinSize:     aws.Int32(0),
				MaxSize:     aws.Int32(4),
				DesiredSize: aws.Int32(3),
			},
			wantInstances: []string{"t3.medium"},
			wantDisk:      20,
			wantMin:       0,
			wantMax:       4,
			wantDesired:   3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerAndBackend(t)
			c := newTestEKSClient(t, h)
			ctx := t.Context()

			_, err := c.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name: aws.String("c"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{"s1"}},
			})
			require.NoError(t, err)

			_, err = c.CreateNodegroup(ctx, &ekssdk.CreateNodegroupInput{
				ClusterName: aws.String("c"), NodegroupName: aws.String("n"),
				NodeRole: aws.String("arn:aws:iam::000000000000:role/n"), Subnets: []string{"s1"},
				ScalingConfig: tc.scaling,
			})
			require.NoError(t, err)

			got, err := c.DescribeNodegroup(
				ctx,
				&ekssdk.DescribeNodegroupInput{ClusterName: aws.String("c"), NodegroupName: aws.String("n")},
			)
			require.NoError(t, err)
			ng := got.Nodegroup
			assert.Equal(t, tc.wantInstances, ng.InstanceTypes)
			assert.Equal(t, tc.wantDisk, aws.ToInt32(ng.DiskSize))
			assert.Equal(t, tc.wantMin, aws.ToInt32(ng.ScalingConfig.MinSize))
			assert.Equal(t, tc.wantMax, aws.ToInt32(ng.ScalingConfig.MaxSize))
			assert.Equal(t, tc.wantDesired, aws.ToInt32(ng.ScalingConfig.DesiredSize))
		})
	}
}

func TestAddon_UpdateAdvancesModifiedAt(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend(t)
	c := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := c.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name: aws.String("c"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{"s1"}},
	})
	require.NoError(t, err)

	created, err := c.CreateAddon(
		ctx,
		&ekssdk.CreateAddonInput{ClusterName: aws.String("c"), AddonName: aws.String("vpc-cni")},
	)
	require.NoError(t, err)
	require.NotNil(t, created.Addon.ModifiedAt)

	_, err = c.UpdateAddon(
		ctx,
		&ekssdk.UpdateAddonInput{
			ClusterName:  aws.String("c"),
			AddonName:    aws.String("vpc-cni"),
			AddonVersion: aws.String("v9"),
		},
	)
	require.NoError(t, err)

	got, err := c.DescribeAddon(
		ctx,
		&ekssdk.DescribeAddonInput{ClusterName: aws.String("c"), AddonName: aws.String("vpc-cni")},
	)
	require.NoError(t, err)
	require.NotNil(t, got.Addon.ModifiedAt)
	assert.False(t, got.Addon.ModifiedAt.Before(*created.Addon.ModifiedAt))
}
