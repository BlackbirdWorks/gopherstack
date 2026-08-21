package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestCreateDescribeCluster_ElasticLoadBalancing_RealClient covers
// gopherstack-tp8x: the real KubernetesNetworkConfigRequest/
// KubernetesNetworkConfigResponse (eks@v1.90.4 types/types.go:1597,1645)
// both declare ElasticLoadBalancing as a sibling of IpFamily/
// ServiceIpv4Cidr/ServiceIpv6Cidr under ONE "kubernetesNetworkConfig" wire
// key. This backend used to split ElasticLoadBalancing into a second,
// separately-named top-level request/response field ("networkingConfig")
// that a real client never reads or sends -- so a real client's
// ElasticLoadBalancing setting inside KubernetesNetworkConfig was silently
// dropped both going in and coming back out. Driven through the real SDK
// client on both CreateCluster and DescribeCluster since the bug is
// symmetric.
func TestCreateDescribeCluster_ElasticLoadBalancing_RealClient(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("tp8x-elb-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
		KubernetesNetworkConfig: &ekstypes.KubernetesNetworkConfigRequest{
			IpFamily:             ekstypes.IpFamilyIpv4,
			ElasticLoadBalancing: &ekstypes.ElasticLoadBalancing{Enabled: aws.Bool(true)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Cluster)
	require.NotNil(t, created.Cluster.KubernetesNetworkConfig)
	require.NotNil(t, created.Cluster.KubernetesNetworkConfig.ElasticLoadBalancing,
		"CreateCluster response must carry ElasticLoadBalancing inside KubernetesNetworkConfig")
	assert.True(t, aws.ToBool(created.Cluster.KubernetesNetworkConfig.ElasticLoadBalancing.Enabled))
	assert.Equal(t, ekstypes.IpFamilyIpv4, created.Cluster.KubernetesNetworkConfig.IpFamily,
		"siblings of ElasticLoadBalancing inside KubernetesNetworkConfig must still round-trip")

	described, err := client.DescribeCluster(ctx, &ekssdk.DescribeClusterInput{Name: aws.String("tp8x-elb-cluster")})
	require.NoError(t, err)
	require.NotNil(t, described.Cluster.KubernetesNetworkConfig)
	require.NotNil(t, described.Cluster.KubernetesNetworkConfig.ElasticLoadBalancing,
		"DescribeCluster response must carry ElasticLoadBalancing inside KubernetesNetworkConfig")
	assert.True(t, aws.ToBool(described.Cluster.KubernetesNetworkConfig.ElasticLoadBalancing.Enabled))
}

// TestCreateCluster_NoElasticLoadBalancing_RealClient guards the common case
// (no load-balancing config supplied) still works after the merge.
func TestCreateCluster_NoElasticLoadBalancing_RealClient(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestEKSClient(t, eks.NewHandler(backend))

	created, err := client.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
		Name:               aws.String("tp8x-no-elb-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Cluster)

	if created.Cluster.KubernetesNetworkConfig != nil {
		assert.Nil(t, created.Cluster.KubernetesNetworkConfig.ElasticLoadBalancing)
	}
}
