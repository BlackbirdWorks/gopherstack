package integration_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EKS_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEKSClient(t)
	ctx := t.Context()

	clusterName := fmt.Sprintf("test-cluster-%s", uuid.NewString()[:8])

	// CreateCluster
	createOut, err := client.CreateCluster(ctx, &eks.CreateClusterInput{
		Name:    aws.String(clusterName),
		Version: aws.String("1.27"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{"subnet-12345678"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(createOut.Cluster.Name))
	assert.NotEmpty(t, aws.ToString(createOut.Cluster.Arn))

	t.Cleanup(func() {
		_, _ = client.DeleteCluster(ctx, &eks.DeleteClusterInput{
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
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("ng-cluster-%s", suffix)
	ngName := fmt.Sprintf("test-ng-%s", suffix)

	// Create cluster first
	_, err := client.CreateCluster(ctx, &eks.CreateClusterInput{
		Name:    aws.String(clusterName),
		Version: aws.String("1.27"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{"subnet-12345678"},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteNodegroup(ctx, &eks.DeleteNodegroupInput{
			ClusterName:   aws.String(clusterName),
			NodegroupName: aws.String(ngName),
		})
		_, _ = client.DeleteCluster(ctx, &eks.DeleteClusterInput{
			Name: aws.String(clusterName),
		})
	})

	// CreateNodegroup
	createOut, err := client.CreateNodegroup(ctx, &eks.CreateNodegroupInput{
		ClusterName:   aws.String(clusterName),
		NodegroupName: aws.String(ngName),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/ng-role"),
		Subnets:       []string{"subnet-12345678"},
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
