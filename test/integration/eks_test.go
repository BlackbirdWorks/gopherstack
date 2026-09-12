package integration_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustCreateEKSSubnet creates a real VPC and subnet via ec2Client so
// ResourcesVpcConfig.SubnetIds/Nodegroup Subnets carry a subnet the EC2
// backend actually knows about, rather than a fabricated literal that would
// break the moment EKS gains an EC2Resolver.SubnetExists check like EFS's
// (gopherstack-1o31). Cleanup for both is registered immediately, before
// the caller creates its EKS resource, so teardown deletes the EKS resource
// first and the VPC/subnet after.
func mustCreateEKSSubnet(t *testing.T, ec2Client *ec2sdk.Client, cidrBase string) string {
	t.Helper()

	ctx := t.Context()

	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String(cidrBase + ".0.0/16"),
	})
	require.NoError(t, err, "CreateVpc should succeed")
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := ec2Client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId:     aws.String(vpcID),
		CidrBlock: aws.String(cidrBase + ".1.0/24"),
	})
	require.NoError(t, err, "CreateSubnet should succeed")
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ec2Client.DeleteSubnet(cleanupCtx, &ec2sdk.DeleteSubnetInput{SubnetId: aws.String(subnetID)})
		_, _ = ec2Client.DeleteVpc(cleanupCtx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpcID)})
	})

	return subnetID
}

func TestIntegration_EKS_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEKSClient(t)
	ec2Client := createEC2Client(t)
	ctx := t.Context()

	clusterName := "test-cluster-" + uuid.NewString()[:8]
	subnetID := mustCreateEKSSubnet(t, ec2Client, "172.20")

	// CreateCluster
	createOut, err := client.CreateCluster(ctx, &eks.CreateClusterInput{
		Name:    aws.String(clusterName),
		Version: aws.String("1.27"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{subnetID},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(createOut.Cluster.Name))
	assert.NotEmpty(t, aws.ToString(createOut.Cluster.Arn))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteCluster(cleanupCtx, &eks.DeleteClusterInput{
			Name: aws.String(clusterName),
		})
	})

	// DescribeCluster
	descOut, err := client.DescribeCluster(ctx, &eks.DescribeClusterInput{
		Name: aws.String(clusterName),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(descOut.Cluster.Name))
	assert.Equal(t, "1.27", aws.ToString(descOut.Cluster.Version))

	// ListClusters
	listOut, err := client.ListClusters(ctx, &eks.ListClustersInput{})
	require.NoError(t, err)
	assert.True(t, slices.Contains(listOut.Clusters, clusterName), "created cluster should appear in ListClusters")

	// DeleteCluster
	delOut, err := client.DeleteCluster(ctx, &eks.DeleteClusterInput{
		Name: aws.String(clusterName),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(delOut.Cluster.Name))

	// Verify deleted
	listOut2, err := client.ListClusters(ctx, &eks.ListClustersInput{})
	require.NoError(t, err)

	for _, name := range listOut2.Clusters {
		assert.NotEqual(t, clusterName, name, "deleted cluster should not appear in list")
	}
}

func TestIntegration_EKS_NodegroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEKSClient(t)
	ec2Client := createEC2Client(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := "ng-cluster-" + suffix
	ngName := "test-ng-" + suffix
	subnetID := mustCreateEKSSubnet(t, ec2Client, "172.21")

	// Create cluster first
	_, err := client.CreateCluster(ctx, &eks.CreateClusterInput{
		Name:    aws.String(clusterName),
		Version: aws.String("1.27"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{subnetID},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteNodegroup(cleanupCtx, &eks.DeleteNodegroupInput{
			ClusterName:   aws.String(clusterName),
			NodegroupName: aws.String(ngName),
		})
		_, _ = client.DeleteCluster(cleanupCtx, &eks.DeleteClusterInput{
			Name: aws.String(clusterName),
		})
	})

	// CreateNodegroup
	createOut, err := client.CreateNodegroup(ctx, &eks.CreateNodegroupInput{
		ClusterName:   aws.String(clusterName),
		NodegroupName: aws.String(ngName),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/ng-role"),
		Subnets:       []string{subnetID},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Nodegroup)
	assert.Equal(t, ngName, aws.ToString(createOut.Nodegroup.NodegroupName))
	assert.Equal(t, clusterName, aws.ToString(createOut.Nodegroup.ClusterName))

	// DescribeNodegroup
	descOut, err := client.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
		ClusterName:   aws.String(clusterName),
		NodegroupName: aws.String(ngName),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Nodegroup)
	assert.Equal(t, ngName, aws.ToString(descOut.Nodegroup.NodegroupName))

	// ListNodegroups
	listOut, err := client.ListNodegroups(ctx, &eks.ListNodegroupsInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)
	assert.True(t, slices.Contains(listOut.Nodegroups, ngName), "created nodegroup should appear in ListNodegroups")

	// DeleteNodegroup
	delOut, err := client.DeleteNodegroup(ctx, &eks.DeleteNodegroupInput{
		ClusterName:   aws.String(clusterName),
		NodegroupName: aws.String(ngName),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.Nodegroup)
	assert.Equal(t, ngName, aws.ToString(delOut.Nodegroup.NodegroupName))

	// Verify deleted
	listOut2, err := client.ListNodegroups(ctx, &eks.ListNodegroupsInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	for _, name := range listOut2.Nodegroups {
		assert.NotEqual(t, ngName, name, "deleted nodegroup should not appear in list")
	}
}
