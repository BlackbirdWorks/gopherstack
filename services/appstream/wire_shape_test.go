package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// This file proves, through the real aws-sdk-go-v2 appstream client
// (appstream@v1.64.5), that two response members and two request members
// decode correctly. Before the fix:
//   - DescribeImagePermissions wrapped its list under "SharedImagePermissions"
//     with PascalCase item fields; the real wire key is
//     "SharedImagePermissionsList" and the SharedImagePermissions/
//     ImagePermissions shapes carry a jsonName override to lowerCamelCase
//     ("sharedAccountId", "imagePermissions", "allowFleet",
//     "allowImageBuilder") -- confirmed against deserializeCBOR_
//     DescribeImagePermissionsOutput/SharedImagePermissions/ImagePermissions
//     in deserializers.go.
//   - AssociateSoftwareToImageBuilder read its software list under "Software";
//     the real input member is "SoftwareNames" -- confirmed against
//     serializeCBOR_AssociateSoftwareToImageBuilderInput in serializers.go.
//   - DescribeSoftwareAssociations read its resource under "ImageBuilderName"
//     (the real member is "AssociatedResource", confirmed against
//     serializeCBOR_DescribeSoftwareAssociationsInput) and emitted list items
//     as {"ImageBuilderName", "Software"}, an entirely different field set
//     than the real {"SoftwareName", "Status", "DeploymentError"}
//     (deserializeCBOR_SoftwareAssociations). Every field of every item, plus
//     the whole request, was silently dropped.
//   - BatchAssociateUserStack/BatchDisassociateUserStack wrapped their
//     per-item error list under "Errors"; the real wire key is lowercase
//     "errors" -- confirmed against deserializeCBOR_BatchAssociateUserStack
//     Output/BatchDisassociateUserStackOutput in deserializers.go. Any
//     per-item association failure was silently invisible to a real client,
//     which would see an empty slice and assume every association in the
//     batch succeeded.
//   - CreateUpdatedImage wrapped its Image under "Image"; the real wire key
//     is lowercase "image" -- confirmed against deserializeCBOR_
//     CreateUpdatedImageOutput in deserializers.go. Note CreateImportedImage's
//     sibling deserializeCBOR_CreateImportedImageOutput genuinely uses
//     "Image" (Pascal) for the same Image type -- the two ops disagree, so
//     inferring one op's casing from the other would have been wrong either
//     way.

func TestSDKRoundTrip_DescribeImagePermissions_SharedImagePermissionsList(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
		Name:        aws.String("perm-src-image"),
		Description: aws.String("test"),
	})
	require.NoError(t, err)

	_, err = client.UpdateImagePermissions(t.Context(), &appstreamsdk.UpdateImagePermissionsInput{
		Name:            aws.String("perm-src-image"),
		SharedAccountId: aws.String("111111111111"),
		ImagePermissions: &types.ImagePermissions{
			AllowFleet:        aws.Bool(true),
			AllowImageBuilder: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeImagePermissions(t.Context(), &appstreamsdk.DescribeImagePermissionsInput{
		Name: aws.String("perm-src-image"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.SharedImagePermissionsList,
		"DescribeImagePermissionsOutput.SharedImagePermissionsList must decode a non-empty slice")

	perm := out.SharedImagePermissionsList[0]
	assert.Equal(t, "111111111111", aws.ToString(perm.SharedAccountId))
	require.NotNil(t, perm.ImagePermissions)
	assert.True(t, aws.ToBool(perm.ImagePermissions.AllowFleet))
	assert.False(t, aws.ToBool(perm.ImagePermissions.AllowImageBuilder))
}

func TestSDKRoundTrip_SoftwareAssociations_WireFields(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateImageBuilder(t.Context(), &appstreamsdk.CreateImageBuilderInput{
		Name:         aws.String("sw-assoc-builder"),
		InstanceType: aws.String("stream.standard.medium"),
		ImageName:    aws.String("some-image"),
	})
	require.NoError(t, err)

	_, err = client.AssociateSoftwareToImageBuilder(t.Context(), &appstreamsdk.AssociateSoftwareToImageBuilderInput{
		ImageBuilderName: aws.String("sw-assoc-builder"),
		SoftwareNames:    []string{"Microsoft_Office_2021_LTSC_Professional_Plus_64Bit"},
	})
	require.NoError(t, err)

	out, err := client.DescribeSoftwareAssociations(t.Context(), &appstreamsdk.DescribeSoftwareAssociationsInput{
		AssociatedResource: aws.String("sw-assoc-builder"),
	})
	require.NoError(t, err)
	assert.Equal(t, "sw-assoc-builder", aws.ToString(out.AssociatedResource))
	require.NotEmpty(t, out.SoftwareAssociations,
		"DescribeSoftwareAssociationsOutput.SoftwareAssociations must decode a non-empty slice")
	assoc := out.SoftwareAssociations[0]
	assert.Equal(t, "Microsoft_Office_2021_LTSC_Professional_Plus_64Bit", aws.ToString(assoc.SoftwareName))
	assert.Equal(t, types.SoftwareDeploymentStatusInstalled, assoc.Status)
}

func TestSDKRoundTrip_BatchAssociateUserStack_ErrorsWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	out, err := client.BatchAssociateUserStack(t.Context(), &appstreamsdk.BatchAssociateUserStackInput{
		UserStackAssociations: []types.UserStackAssociation{
			{
				UserName:           aws.String("ghost@example.com"),
				StackName:          aws.String("any-stack"),
				AuthenticationType: types.AuthenticationTypeUserpool,
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Errors,
		"BatchAssociateUserStackOutput.Errors must decode a non-empty slice for an unknown user")
	assert.Equal(t, "ghost@example.com", aws.ToString(out.Errors[0].UserStackAssociation.UserName))
}

func TestSDKRoundTrip_CreateUpdatedImage_ImageWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
		Name:        aws.String("base-img"),
		Description: aws.String("test"),
	})
	require.NoError(t, err)

	out, err := client.CreateUpdatedImage(t.Context(), &appstreamsdk.CreateUpdatedImageInput{
		ExistingImageName: aws.String("base-img"),
		NewImageName:      aws.String("updated-img"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Image, "CreateUpdatedImageOutput.Image must decode a non-nil Image")
	assert.Equal(t, "updated-img", aws.ToString(out.Image.Name))
}
