package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func newENIFixture(t *testing.T) (*sagemakersdk.Client, *sagemaker.InMemoryBackend, []string) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("eni-cluster"),
		InstanceGroups: []smtypes.ClusterInstanceGroupSpecification{{
			InstanceGroupName: aws.String("workers"),
			InstanceCount:     aws.Int32(2),
			InstanceType:      smtypes.ClusterInstanceTypeMlG5Xlarge,
			ExecutionRole:     aws.String("arn:aws:iam::000000000000:role/hp"),
			LifeCycleConfig: &smtypes.ClusterLifeCycleConfig{
				SourceS3Uri: aws.String("s3://b/lc"), OnCreate: aws.String("on.sh"),
			},
		}},
	})
	require.NoError(t, err)

	nodes, err := client.ListClusterNodes(t.Context(), &sagemakersdk.ListClusterNodesInput{
		ClusterName: aws.String("eni-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, nodes.ClusterNodeSummaries, 2)

	ids := []string{
		aws.ToString(nodes.ClusterNodeSummaries[0].InstanceId),
		aws.ToString(nodes.ClusterNodeSummaries[1].InstanceId),
	}

	return client, h.Backend, ids
}

func TestSDKRoundTrip_AttachClusterNodeNetworkInterface(t *testing.T) {
	t.Parallel()

	client, _, nodes := newENIFixture(t)
	ctx := t.Context()

	out, err := client.AttachClusterNodeNetworkInterface(ctx, &sagemakersdk.AttachClusterNodeNetworkInterfaceInput{
		ClusterName: aws.String(
			"eni-cluster",
		),
		NodeId:             aws.String(nodes[0]),
		NetworkInterfaceId: aws.String("eni-0abc"),
	})
	require.NoError(t, err)
	assert.Equal(t, nodes[0], aws.ToString(out.NodeId))
	assert.Equal(t, "eni-0abc", aws.ToString(out.NetworkInterfaceId))
	assert.Contains(t, aws.ToString(out.ClusterArn), ":cluster/eni-cluster")
	assert.NotEmpty(t, aws.ToString(out.AttachmentId))

	second, err := client.AttachClusterNodeNetworkInterface(ctx, &sagemakersdk.AttachClusterNodeNetworkInterfaceInput{
		ClusterName: out.ClusterArn, NodeId: aws.String(nodes[0]), NetworkInterfaceId: aws.String("eni-0def"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(out.AttachmentId), aws.ToString(second.AttachmentId))
}

func TestSDKRoundTrip_AttachClusterNodeNetworkInterface_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       *sagemakersdk.AttachClusterNodeNetworkInterfaceInput
		name     string
		wantCode string
		first    bool
	}{
		{
			name:     "unknown_cluster",
			in:       eniInput("nope", "node-1", "eni-1"),
			wantCode: "ResourceNotFound",
		},
		{
			name:     "unknown_node",
			in:       eniInput("eni-cluster", "i-missing", "eni-1"),
			wantCode: "ResourceNotFound",
		},
		{
			name:     "eni_already_attached",
			first:    true,
			in:       eniInput("eni-cluster", "", "eni-dup"),
			wantCode: "ResourceInUse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _, nodes := newENIFixture(t)

			in := tt.in
			if in.NodeId != nil && aws.ToString(in.NodeId) == "" {
				in.NodeId = aws.String(nodes[1])
			}

			if tt.first {
				_, err := client.AttachClusterNodeNetworkInterface(
					t.Context(), eniInput("eni-cluster", nodes[0], "eni-dup"),
				)
				require.NoError(t, err)
			}

			_, err := client.AttachClusterNodeNetworkInterface(t.Context(), in)
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func eniInput(cluster, node, eni string) *sagemakersdk.AttachClusterNodeNetworkInterfaceInput {
	return &sagemakersdk.AttachClusterNodeNetworkInterfaceInput{
		ClusterName: aws.String(cluster), NodeId: aws.String(node), NetworkInterfaceId: aws.String(eni),
	}
}

func TestAttachClusterNodeNetworkInterface_Persistence(t *testing.T) {
	t.Parallel()

	client, src, nodes := newENIFixture(t)

	_, err := client.AttachClusterNodeNetworkInterface(t.Context(), eniInput("eni-cluster", nodes[0], "eni-keep"))
	require.NoError(t, err)

	dst := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	restored := newTestSageMakerClient(t, sagemaker.NewHandler(dst))
	_, err = restored.AttachClusterNodeNetworkInterface(t.Context(), eniInput("eni-cluster", nodes[1], "eni-keep"))
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceInUse", apiErr.ErrorCode())
}

func TestSDKRoundTrip_AttachClusterNodeNetworkInterface_RestrictedGroup(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	_, err := client.CreateCluster(t.Context(), &sagemakersdk.CreateClusterInput{
		ClusterName: aws.String("rig-cluster"),
		RestrictedInstanceGroups: []smtypes.ClusterRestrictedInstanceGroupSpecification{{
			InstanceGroupName: aws.String("rig"),
			InstanceCount:     aws.Int32(1),
			InstanceType:      smtypes.ClusterInstanceTypeMlG5Xlarge,
			ExecutionRole:     aws.String("arn:aws:iam::000000000000:role/hp"),
			EnvironmentConfig: &smtypes.EnvironmentConfig{
				FSxLustreConfig: &smtypes.FSxLustreConfig{
					SizeInGiB:                aws.Int32(1200),
					PerUnitStorageThroughput: aws.Int32(250),
				},
			},
		}},
		RestrictedInstanceGroupsConfig: &smtypes.ClusterRestrictedInstanceGroupsConfig{
			SharedEnvironmentConfig: &smtypes.ClusterSharedEnvironmentConfig{
				FSxLustreConfig: &smtypes.FSxLustreConfig{
					SizeInGiB:                aws.Int32(1200),
					PerUnitStorageThroughput: aws.Int32(250),
				},
				FSxLustreDeletionPolicy: smtypes.ClusterFSxLustreDeletionPolicyKeep,
			},
		},
	})
	require.NoError(t, err)

	nodes, err := client.ListClusterNodes(
		t.Context(),
		&sagemakersdk.ListClusterNodesInput{ClusterName: aws.String("rig-cluster")},
	)
	require.NoError(t, err)
	require.Len(t, nodes.ClusterNodeSummaries, 1)

	_, err = client.AttachClusterNodeNetworkInterface(
		t.Context(), eniInput("rig-cluster", aws.ToString(nodes.ClusterNodeSummaries[0].InstanceId), "eni-rig"),
	)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
}
