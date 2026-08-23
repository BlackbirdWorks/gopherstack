package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3AccessPoint_WireShape proves gopherstack's CreateAndAttachS3AccessPoint and
// DescribeS3AccessPointAttachments wrap their response under the real op's own key
// -- "S3AccessPointAttachment"/"S3AccessPointAttachments" -- and nested shape
// (types.S3AccessPointAttachment, types/types.go:3898), not under a "S3AccessPoint"/
// "S3AccessPoints" key with a flat FileSystemId/VolumeId/Tags shape that neither op's
// real Output struct has (fsx@v1.68.4 api_op_CreateAndAttachS3AccessPoint.go:
// CreateAndAttachS3AccessPointOutput{S3AccessPointAttachment}; deserializers.go:15957
// confirms the live per-op deserializer switch has no case "S3AccessPoint" -- only
// "S3AccessPointAttachment"). A real client wrapping under the wrong key gets nil back.
func TestS3AccessPoint_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	volOut, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
		VolumeType: types.VolumeTypeOntap,
		Name:       aws.String("ap-vol"),
	})
	require.NoError(t, err)

	createOut, err := client.CreateAndAttachS3AccessPoint(t.Context(), &fsxsdk.CreateAndAttachS3AccessPointInput{
		Name: aws.String("my-ap"),
		Type: types.S3AccessPointAttachmentTypeOntap,
		OntapConfiguration: &types.CreateAndAttachS3AccessPointOntapConfiguration{
			VolumeId: volOut.Volume.VolumeId,
			FileSystemIdentity: &types.OntapFileSystemIdentity{
				Type: types.OntapFileSystemUserTypeUnix,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.S3AccessPointAttachment)
	assert.Equal(t, "my-ap", aws.ToString(createOut.S3AccessPointAttachment.Name))
	assert.NotEmpty(t, createOut.S3AccessPointAttachment.Lifecycle)
	require.NotNil(t, createOut.S3AccessPointAttachment.OntapConfiguration)
	assert.Equal(t, aws.ToString(volOut.Volume.VolumeId),
		aws.ToString(createOut.S3AccessPointAttachment.OntapConfiguration.VolumeId))
	require.NotNil(t, createOut.S3AccessPointAttachment.S3AccessPoint)
	assert.NotEmpty(t, aws.ToString(createOut.S3AccessPointAttachment.S3AccessPoint.ResourceARN))

	describeOut, err := client.DescribeS3AccessPointAttachments(
		t.Context(),
		&fsxsdk.DescribeS3AccessPointAttachmentsInput{Names: []string{"my-ap"}},
	)
	require.NoError(t, err)
	require.Len(t, describeOut.S3AccessPointAttachments, 1)
	assert.Equal(t, "my-ap", aws.ToString(describeOut.S3AccessPointAttachments[0].Name))

	_, err = client.DetachAndDeleteS3AccessPoint(t.Context(), &fsxsdk.DetachAndDeleteS3AccessPointInput{
		Name: aws.String("my-ap"),
	})
	require.NoError(t, err, "real DetachAndDeleteS3AccessPointInput has no FileSystemId member at all")
}
