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
	assert.Equal(t, types.UserStackAssociationErrorCodeUserNameNotFound, out.Errors[0].ErrorCode)
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

// TestSDKRoundTrip_DeleteImage_ImageWireKey proves DeleteImage returns the
// deleted Image rather than an empty envelope. Real AWS's DeleteImageOutput
// carries the deleted Image under "Image" (deserializeCBOR_DeleteImageOutput
// in deserializers.go) -- the same empty-envelope bug class as ec2's
// DeleteLaunchTemplate.
func TestSDKRoundTrip_DeleteImage_ImageWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
		Name: aws.String("del-img"),
	})
	require.NoError(t, err)

	out, err := client.DeleteImage(t.Context(), &appstreamsdk.DeleteImageInput{
		Name: aws.String("del-img"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Image, "DeleteImageOutput.Image must decode a non-nil Image")
	assert.Equal(t, "del-img", aws.ToString(out.Image.Name))
}

// TestSDKRoundTrip_DeleteImageBuilder_ImageBuilderWireKey proves
// DeleteImageBuilder returns the full deleted ImageBuilder shape rather than
// an empty envelope or a stripped-down Name/ImageName-only object. Real
// AWS's DeleteImageBuilderOutput carries the deleted ImageBuilder under
// "ImageBuilder" (deserializeCBOR_DeleteImageBuilderOutput in
// deserializers.go).
func TestSDKRoundTrip_DeleteImageBuilder_ImageBuilderWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateImageBuilder(t.Context(), &appstreamsdk.CreateImageBuilderInput{
		Name:         aws.String("del-builder"),
		InstanceType: aws.String("stream.standard.medium"),
	})
	require.NoError(t, err)

	out, err := client.DeleteImageBuilder(t.Context(), &appstreamsdk.DeleteImageBuilderInput{
		Name: aws.String("del-builder"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ImageBuilder, "DeleteImageBuilderOutput.ImageBuilder must decode a non-nil ImageBuilder")
	assert.Equal(t, "del-builder", aws.ToString(out.ImageBuilder.Name))
	assert.Equal(t, "stream.standard.medium", aws.ToString(out.ImageBuilder.InstanceType))
}

// TestSDKRoundTrip_AssociateApplicationFleet_AssociationWireKey proves
// AssociateApplicationFleet returns the created association rather than an
// empty envelope. Real AWS's AssociateApplicationFleetOutput carries the
// ApplicationFleetAssociation under "ApplicationFleetAssociation"
// (deserializeCBOR_AssociateApplicationFleetOutput in deserializers.go).
func TestSDKRoundTrip_AssociateApplicationFleet_AssociationWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateApplication(t.Context(), &appstreamsdk.CreateApplicationInput{
		Name:       aws.String("assoc-app"),
		LaunchPath: aws.String("/app/assoc-app"),
		IconS3Location: &types.S3Location{
			S3Bucket: aws.String("icon-bucket"),
			S3Key:    aws.String("icons/assoc-app.png"),
		},
		Platforms:        []types.PlatformType{types.PlatformTypeWindowsServer2019},
		AppBlockArn:      aws.String("arn:aws:appstream:us-east-1:123456789012:app-block/assoc-app-block"),
		InstanceFamilies: []string{"GENERAL_PURPOSE"},
	})
	require.NoError(t, err)

	_, err = client.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
		Name:         aws.String("assoc-fleet"),
		InstanceType: aws.String("stream.standard.medium"),
	})
	require.NoError(t, err)

	out, err := client.AssociateApplicationFleet(t.Context(), &appstreamsdk.AssociateApplicationFleetInput{
		ApplicationArn: aws.String("assoc-app"),
		FleetName:      aws.String("assoc-fleet"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ApplicationFleetAssociation,
		"AssociateApplicationFleetOutput.ApplicationFleetAssociation must decode non-nil")
	assert.Equal(t, "assoc-fleet", aws.ToString(out.ApplicationFleetAssociation.FleetName))
}

// TestSDKRoundTrip_AssociateAppBlockBuilderAppBlock_AssociationWireKey
// proves AssociateAppBlockBuilderAppBlock returns the created association
// rather than an empty envelope. Real AWS's
// AssociateAppBlockBuilderAppBlockOutput carries the
// AppBlockBuilderAppBlockAssociation under
// "AppBlockBuilderAppBlockAssociation"
// (deserializeCBOR_AssociateAppBlockBuilderAppBlockOutput in
// deserializers.go).
func TestSDKRoundTrip_AssociateAppBlockBuilderAppBlock_AssociationWireKey(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateAppBlock(t.Context(), &appstreamsdk.CreateAppBlockInput{
		Name: aws.String("assoc-appblock"),
		SourceS3Location: &types.S3Location{
			S3Bucket: aws.String("appblock-bucket"),
			S3Key:    aws.String("appblocks/assoc-appblock.zip"),
		},
	})
	require.NoError(t, err)

	_, err = client.CreateAppBlockBuilder(t.Context(), &appstreamsdk.CreateAppBlockBuilderInput{
		Name:         aws.String("assoc-builder"),
		InstanceType: aws.String("stream.standard.medium"),
		Platform:     types.AppBlockBuilderPlatformTypeWindowsServer2019,
		VpcConfig: &types.VpcConfig{
			SubnetIds: []string{"subnet-1", "subnet-2"},
		},
	})
	require.NoError(t, err)

	out, err := client.AssociateAppBlockBuilderAppBlock(
		t.Context(),
		&appstreamsdk.AssociateAppBlockBuilderAppBlockInput{
			AppBlockBuilderName: aws.String("assoc-builder"),
			AppBlockArn:         aws.String("assoc-appblock"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.AppBlockBuilderAppBlockAssociation,
		"AssociateAppBlockBuilderAppBlockOutput.AppBlockBuilderAppBlockAssociation must decode non-nil")
	assert.Equal(t, "assoc-builder", aws.ToString(out.AppBlockBuilderAppBlockAssociation.AppBlockBuilderName))
}

// TestSDKRoundTrip_UpdateThemeForStack_FieldsApply proves UpdateThemeForStack
// actually applies its request fields instead of silently discarding every
// one of them (the handler previously decoded only StackName -- see
// themes.go/handler_user.go). Real UpdateThemeForStackInput
// (api_op_UpdateThemeForStack.go:29-64) carries ThemeStyling/TitleText/
// FaviconS3Location/OrganizationLogoS3Location/FooterLinks/State/
// AttributesToDelete, all optional except StackName ("Specify the fields you
// want to update. Omitted fields are unchanged" is this op's own doc
// convention, confirmed structurally by every field but StackName being
// absent from validators.go's required-field checks).
func TestSDKRoundTrip_UpdateThemeForStack_FieldsApply(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestAppStreamClient(t, h)

	_, err := client.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{Name: aws.String("theme-upd-stack")})
	require.NoError(t, err)

	_, err = client.CreateThemeForStack(t.Context(), &appstreamsdk.CreateThemeForStackInput{
		StackName: aws.String("theme-upd-stack"),
		FaviconS3Location: &types.S3Location{
			S3Bucket: aws.String("theme-assets"),
			S3Key:    aws.String("favicon.ico"),
		},
		OrganizationLogoS3Location: &types.S3Location{
			S3Bucket: aws.String("theme-assets"),
			S3Key:    aws.String("logo.png"),
		},
		ThemeStyling: types.ThemeStylingBlue,
		TitleText:    aws.String("Original Title"),
		FooterLinks: []types.ThemeFooterLink{
			{DisplayName: aws.String("Support"), FooterLinkURL: aws.String("https://support.example.com")},
		},
	})
	require.NoError(t, err)

	out, err := client.UpdateThemeForStack(t.Context(), &appstreamsdk.UpdateThemeForStackInput{
		StackName: aws.String("theme-upd-stack"),
		FaviconS3Location: &types.S3Location{
			S3Bucket: aws.String("theme-assets"),
			S3Key:    aws.String("new-favicon.ico"),
		},
		ThemeStyling: types.ThemeStylingRed,
		TitleText:    aws.String("Updated Title"),
		State:        types.ThemeStateDisabled,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Theme, "UpdateThemeForStackOutput.Theme must decode non-nil")
	assert.Equal(t, "https://s3.amazonaws.com/theme-assets/new-favicon.ico", aws.ToString(out.Theme.ThemeFaviconURL),
		"FaviconS3Location must actually be applied by UpdateThemeForStack, not silently dropped")
	assert.Equal(t, types.ThemeStylingRed, out.Theme.ThemeStyling,
		"ThemeStyling must actually be applied by UpdateThemeForStack, not silently dropped")
	assert.Equal(t, "Updated Title", aws.ToString(out.Theme.ThemeTitleText),
		"TitleText must actually be applied by UpdateThemeForStack, not silently dropped")
	assert.Equal(t, types.ThemeStateDisabled, out.Theme.State,
		"State must actually be applied by UpdateThemeForStack, not silently dropped")
	// OrganizationLogoS3Location was omitted from the update request -- must stay unchanged.
	assert.Equal(t, "https://s3.amazonaws.com/theme-assets/logo.png",
		aws.ToString(out.Theme.ThemeOrganizationLogoURL),
		"an omitted field must be left unchanged, not cleared")
	// FooterLinks was omitted from the update request -- must stay unchanged.
	require.Len(t, out.Theme.ThemeFooterLinks, 1)
	assert.Equal(t, "Support", aws.ToString(out.Theme.ThemeFooterLinks[0].DisplayName))

	// AttributesToDelete=[FOOTER_LINKS] clears the footer links.
	out2, err := client.UpdateThemeForStack(t.Context(), &appstreamsdk.UpdateThemeForStackInput{
		StackName:          aws.String("theme-upd-stack"),
		AttributesToDelete: []types.ThemeAttribute{types.ThemeAttributeFooterLinks},
	})
	require.NoError(t, err)
	assert.Empty(t, out2.Theme.ThemeFooterLinks,
		"AttributesToDelete=[FOOTER_LINKS] must clear ThemeFooterLinks")
	// Confirms the fix isn't just clearing everything: TitleText from the prior update survives.
	assert.Equal(t, "Updated Title", aws.ToString(out2.Theme.ThemeTitleText))
}
