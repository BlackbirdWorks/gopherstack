package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_AttachDetachClusterNodeVolume_RealClient proves
// AttachClusterNodeVolumeInput really carries ClusterArn/NodeId/VolumeId
// (api_op_AttachClusterNodeVolume.go:33-51, sagemaker@v1.263.2) — not
// ClusterName/VolumeConfig — and that the response surfaces the required
// AttachTime/ClusterArn/DeviceName/NodeId/Status/VolumeId members
// (api_op_AttachClusterNodeVolume.go:60-93). Before the fix, the real SDK
// client's request body carries "ClusterArn" but the handler looked for
// "ClusterName", so every real client call failed with 400 ValidationException
// ("ClusterName is required") even though the request was correctly formed.
func TestHandler_AttachDetachClusterNodeVolume_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	cluster := h.Backend.AddClusterInternal(t.Context(), "vol-cluster")

	attachOut, err := client.AttachClusterNodeVolume(t.Context(), &sagemakersdk.AttachClusterNodeVolumeInput{
		ClusterArn: aws.String(cluster.ClusterArn),
		NodeId:     aws.String("node-1"),
		VolumeId:   aws.String("vol-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, cluster.ClusterArn, aws.ToString(attachOut.ClusterArn))
	assert.Equal(t, "node-1", aws.ToString(attachOut.NodeId))
	assert.Equal(t, "vol-1", aws.ToString(attachOut.VolumeId))
	assert.Equal(t, smtypes.VolumeAttachmentStatus("attached"), attachOut.Status)
	assert.NotEmpty(t, aws.ToString(attachOut.DeviceName))
	assert.NotNil(t, attachOut.AttachTime)

	detachOut, err := client.DetachClusterNodeVolume(t.Context(), &sagemakersdk.DetachClusterNodeVolumeInput{
		ClusterArn: aws.String(cluster.ClusterArn),
		NodeId:     aws.String("node-1"),
		VolumeId:   aws.String("vol-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "vol-1", aws.ToString(detachOut.VolumeId))
}

// TestHandler_DescribeSpace_Status_RealClient proves DescribeSpaceOutput's
// status member is wire key "Status" (deserializers.go:118584, confirmed
// against sagemaker@v1.263.2's awsAwsjson11_deserializeOpDocumentDescribeSpaceOutput),
// not "SpaceStatus". Before the fix, the handler emitted "SpaceStatus", which
// the real client's deserializer does not recognize (it falls into the
// generic `default` case and is discarded), so DescribeSpaceOutput.Status
// always came back as the empty string despite the space being InService.
func TestHandler_DescribeSpace_Status_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateSpace(t.Context(), &sagemakersdk.CreateSpaceInput{
		DomainId:  aws.String("d-1"),
		SpaceName: aws.String("space-1"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeSpace(t.Context(), &sagemakersdk.DescribeSpaceInput{
		DomainId:  aws.String("d-1"),
		SpaceName: aws.String("space-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.SpaceStatusInService, desc.Status)

	list, err := client.ListSpaces(t.Context(), &sagemakersdk.ListSpacesInput{})
	require.NoError(t, err)
	require.Len(t, list.Spaces, 1)
	assert.Equal(t, smtypes.SpaceStatusInService, list.Spaces[0].Status)
}
