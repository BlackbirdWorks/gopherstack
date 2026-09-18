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

// TestNodegroup_ModifiedAtAndUpdateStrategy_RealClient drives CreateNodegroup
// and UpdateNodegroupConfig through the real aws-sdk-go-v2 client.
// types.Nodegroup.ModifiedAt (eks@v1.98.0 deserializers.go, case "modifiedAt")
// was never emitted at all -- Nodegroup had no backing field -- so every
// real client's DescribeNodegroup/ListNodegroups decode left it nil forever.
// types.NodegroupUpdateConfig.UpdateStrategy (case "updateStrategy") was
// accepted on neither Create nor Update and never echoed back.
func TestNodegroup_ModifiedAtAndUpdateStrategy_RealClient(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:    aws.String("ng-modifiedat-cluster"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{
			SubnetIds: []string{"subnet-abc123"},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateNodegroup(ctx, &ekssdk.CreateNodegroupInput{
		ClusterName:   aws.String("ng-modifiedat-cluster"),
		NodegroupName: aws.String("ng1"),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/ng-role"),
		Subnets:       []string{"subnet-abc123"},
		UpdateConfig: &ekstypes.NodegroupUpdateConfig{
			UpdateStrategy: ekstypes.NodegroupUpdateStrategiesMinimal,
		},
	})
	require.NoError(t, err)

	require.NotNil(t, created.Nodegroup.ModifiedAt, "ModifiedAt must be set on create")
	assert.False(t, created.Nodegroup.ModifiedAt.IsZero())
	require.NotNil(t, created.Nodegroup.UpdateConfig)
	assert.Equal(t, ekstypes.NodegroupUpdateStrategiesMinimal, created.Nodegroup.UpdateConfig.UpdateStrategy)

	createdModifiedAt := *created.Nodegroup.ModifiedAt

	_, err = client.UpdateNodegroupConfig(ctx, &ekssdk.UpdateNodegroupConfigInput{
		ClusterName:   aws.String("ng-modifiedat-cluster"),
		NodegroupName: aws.String("ng1"),
		UpdateConfig: &ekstypes.NodegroupUpdateConfig{
			UpdateStrategy: ekstypes.NodegroupUpdateStrategiesDefault,
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeNodegroup(ctx, &ekssdk.DescribeNodegroupInput{
		ClusterName:   aws.String("ng-modifiedat-cluster"),
		NodegroupName: aws.String("ng1"),
	})
	require.NoError(t, err)

	require.NotNil(t, described.Nodegroup.ModifiedAt)
	assert.False(t, described.Nodegroup.ModifiedAt.Before(createdModifiedAt),
		"ModifiedAt must never move backward after UpdateNodegroupConfig")
	require.NotNil(t, described.Nodegroup.UpdateConfig)
	assert.Equal(t, ekstypes.NodegroupUpdateStrategiesDefault, described.Nodegroup.UpdateConfig.UpdateStrategy)
}
