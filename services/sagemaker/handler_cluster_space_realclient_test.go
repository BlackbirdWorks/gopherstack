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

// TestHandler_UpdateSpace_RealClient proves UpdateSpaceInput really carries
// SpaceDisplayName/SpaceSettings (api_op_UpdateSpace.go:27-42,
// sagemaker@v1.263.2) and that DescribeSpace reflects the update. Before the
// fix, the handler's updateSpaceInput struct declared only DomainId/SpaceName,
// so a real client's SpaceDisplayName/SpaceSettings were silently dropped by
// json.Unmarshal: UpdateSpace returned 200 OK but left the space unchanged.
func TestHandler_UpdateSpace_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateSpace(t.Context(), &sagemakersdk.CreateSpaceInput{
		DomainId:         aws.String("d-1"),
		SpaceName:        aws.String("space-1"),
		SpaceDisplayName: aws.String("Original Name"),
		SpaceSettings:    &smtypes.SpaceSettings{AppType: smtypes.AppTypeJupyterLab},
	})
	require.NoError(t, err)

	_, err = client.UpdateSpace(t.Context(), &sagemakersdk.UpdateSpaceInput{
		DomainId:         aws.String("d-1"),
		SpaceName:        aws.String("space-1"),
		SpaceDisplayName: aws.String("Updated Name"),
		SpaceSettings:    &smtypes.SpaceSettings{AppType: smtypes.AppTypeCodeEditor},
	})
	require.NoError(t, err)

	desc, err := client.DescribeSpace(t.Context(), &sagemakersdk.DescribeSpaceInput{
		DomainId:  aws.String("d-1"),
		SpaceName: aws.String("space-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", aws.ToString(desc.SpaceDisplayName))
	require.NotNil(t, desc.SpaceSettings)
	assert.Equal(t, smtypes.AppTypeCodeEditor, desc.SpaceSettings.AppType)
}

// TestHandler_UpdateUserProfile_RealClient proves UpdateUserProfileInput
// really carries UserSettings (api_op_UpdateUserProfile.go:26-38,
// sagemaker@v1.263.2) and that DescribeUserProfile reflects the update.
// Before the fix, the handler's updateUserProfileInput struct declared only
// DomainId/UserProfileName, so a real client's UserSettings was silently
// dropped by json.Unmarshal: UpdateUserProfile returned 200 OK but left the
// profile's settings unchanged.
func TestHandler_UpdateUserProfile_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateUserProfile(t.Context(), &sagemakersdk.CreateUserProfileInput{
		DomainId:        aws.String("d-1"),
		UserProfileName: aws.String("user-1"),
		UserSettings:    &smtypes.UserSettings{ExecutionRole: aws.String("arn:aws:iam::123456789012:role/original")},
	})
	require.NoError(t, err)

	_, err = client.UpdateUserProfile(t.Context(), &sagemakersdk.UpdateUserProfileInput{
		DomainId:        aws.String("d-1"),
		UserProfileName: aws.String("user-1"),
		UserSettings:    &smtypes.UserSettings{ExecutionRole: aws.String("arn:aws:iam::123456789012:role/updated")},
	})
	require.NoError(t, err)

	desc, err := client.DescribeUserProfile(t.Context(), &sagemakersdk.DescribeUserProfileInput{
		DomainId:        aws.String("d-1"),
		UserProfileName: aws.String("user-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.UserSettings)
	assert.Equal(t, "arn:aws:iam::123456789012:role/updated", aws.ToString(desc.UserSettings.ExecutionRole))
}
