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

func newMembersClient(t *testing.T) *appstreamsdk.Client {
	t.Helper()

	return newTestAppStreamClient(t, appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1")))
}

func vpc() *types.VpcConfig {
	return &types.VpcConfig{SecurityGroupIds: []string{"sg-1"}, SubnetIds: []string{"subnet-1"}}
}

func TestRealClient_AppBlockBuilderMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *appstreamsdk.Client)
		name string
	}{
		{name: "create_and_update", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			created, err := c.CreateAppBlockBuilder(t.Context(), &appstreamsdk.CreateAppBlockBuilderInput{
				Name:          aws.String("bb"),
				Platform:      types.AppBlockBuilderPlatformTypeWindowsServer2019,
				InstanceType:  aws.String("stream.standard.medium"),
				VpcConfig:     vpc(),
				DisplayName:   aws.String("Builder"),
				IamRoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
				DisableIMDSV1: aws.Bool(true),
				AccessEndpoints: []types.AccessEndpoint{
					{EndpointType: types.AccessEndpointTypeStreaming, VpceId: aws.String("vpce-1")},
				},
			})
			require.NoError(t, err)
			bb := created.AppBlockBuilder
			assert.Equal(t, "Builder", aws.ToString(bb.DisplayName))
			assert.Equal(t, "arn:aws:iam::000000000000:role/r", aws.ToString(bb.IamRoleArn))
			assert.True(t, aws.ToBool(bb.DisableIMDSV1))
			require.Len(t, bb.AccessEndpoints, 1)

			upd, err := c.UpdateAppBlockBuilder(t.Context(), &appstreamsdk.UpdateAppBlockBuilderInput{
				Name:        aws.String("bb"),
				DisplayName: aws.String("Renamed"),
				AttributesToDelete: []types.AppBlockBuilderAttribute{
					types.AppBlockBuilderAttributeIamRoleArn,
					types.AppBlockBuilderAttributeAccessEndpoints,
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "Renamed", aws.ToString(upd.AppBlockBuilder.DisplayName))
			assert.Empty(t, aws.ToString(upd.AppBlockBuilder.IamRoleArn))
			assert.Empty(t, upd.AppBlockBuilder.AccessEndpoints)
			assert.True(t, aws.ToBool(upd.AppBlockBuilder.DisableIMDSV1), "omitted member keeps its value")
		}},
		{name: "bad_attribute", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateAppBlockBuilder(t.Context(), &appstreamsdk.CreateAppBlockBuilderInput{
				Name: aws.String("bb2"), Platform: types.AppBlockBuilderPlatformTypeWindowsServer2019,
				InstanceType: aws.String("stream.standard.medium"), VpcConfig: vpc(),
			})
			require.NoError(t, err)

			_, err = c.UpdateAppBlockBuilder(t.Context(), &appstreamsdk.UpdateAppBlockBuilderInput{
				Name:               aws.String("bb2"),
				AttributesToDelete: []types.AppBlockBuilderAttribute{"BOGUS"},
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newMembersClient(t))
		})
	}
}

func TestRealClient_ImageBuilderSourceImageAndSoftware(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)

	_, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
		Name: aws.String("base-img"), DisplayName: aws.String("Base"),
	})
	require.NoError(t, err)

	out, err := c.CreateImageBuilder(t.Context(), &appstreamsdk.CreateImageBuilderInput{
		Name:                 aws.String("ib-1"),
		InstanceType:         aws.String("stream.standard.medium"),
		ImageName:            aws.String("base-img"),
		DisplayName:          aws.String("Builder One"),
		SoftwaresToInstall:   []string{"Microsoft_Project_2021_Standard_64Bit", "Other_App"},
		SoftwaresToUninstall: []string{"Other_App"},
	})
	require.NoError(t, err)
	assert.Equal(t, "Builder One", aws.ToString(out.ImageBuilder.DisplayName))
	assert.Contains(t, aws.ToString(out.ImageBuilder.ImageArn), "image/base-img")

	assoc, err := c.DescribeSoftwareAssociations(t.Context(), &appstreamsdk.DescribeSoftwareAssociationsInput{
		AssociatedResource: aws.String("ib-1"),
	})
	require.NoError(t, err)
	require.Len(t, assoc.SoftwareAssociations, 1)
	assert.Equal(t, "Microsoft_Project_2021_Standard_64Bit", aws.ToString(assoc.SoftwareAssociations[0].SoftwareName))
}

func TestRealClient_UpdatedAndImportedImageMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *appstreamsdk.Client)
		name string
	}{
		{name: "updated_image_display_name_tags", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("src"),
			})
			require.NoError(t, err)

			out, err := c.CreateUpdatedImage(t.Context(), &appstreamsdk.CreateUpdatedImageInput{
				ExistingImageName:   aws.String("src"),
				NewImageName:        aws.String("dst"),
				NewImageDisplayName: aws.String("Destination"),
				NewImageTags:        map[string]string{"env": "dev"},
			})
			require.NoError(t, err)
			assert.Equal(t, "Destination", aws.ToString(out.Image.DisplayName))

			tags, err := c.ListTagsForResource(t.Context(), &appstreamsdk.ListTagsForResourceInput{
				ResourceArn: out.Image.Arn,
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "dev"}, tags.Tags)
		}},
		{name: "updated_image_dry_run_creates_nothing", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("src"),
			})
			require.NoError(t, err)

			out, err := c.CreateUpdatedImage(t.Context(), &appstreamsdk.CreateUpdatedImageInput{
				ExistingImageName: aws.String("src"), NewImageName: aws.String("dst"), DryRun: aws.Bool(true),
			})
			require.NoError(t, err)
			assert.True(t, aws.ToBool(out.CanUpdateImage))

			list, err := c.DescribeImages(t.Context(), &appstreamsdk.DescribeImagesInput{})
			require.NoError(t, err)
			assert.Len(t, list.Images, 1)
		}},
		{name: "imported_image_display_name_and_dry_run", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			out, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("imp"), DisplayName: aws.String("Imported"),
			})
			require.NoError(t, err)
			assert.Equal(t, "Imported", aws.ToString(out.Image.DisplayName))

			_, err = c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("imp"), DryRun: aws.Bool(true),
			})
			require.Error(t, err, "dry run still validates the name")

			dry, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("other"), DryRun: aws.Bool(true),
			})
			require.NoError(t, err)
			assert.Nil(t, dry.Image)

			list, err := c.DescribeImages(t.Context(), &appstreamsdk.DescribeImagesInput{})
			require.NoError(t, err)
			assert.Len(t, list.Images, 1)
		}},
		{name: "imported_image_bad_agent_version", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateImportedImage(t.Context(), &appstreamsdk.CreateImportedImageInput{
				Name: aws.String("bad"), AgentSoftwareVersion: "NEVER",
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newMembersClient(t))
		})
	}
}

func TestRealClient_StackAgentAccessConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *appstreamsdk.Client)
		name string
	}{
		{name: "create_update_delete", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			created, err := c.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{
				Name: aws.String("agent-stack"),
				AgentAccessConfig: &types.AgentAccessConfig{
					ScreenImageFormat: types.ScreenImageFormatPng,
					ScreenResolution:  types.ScreenResolutionW1280xH720,
					UserControlMode:   types.UserControlModeViewOnly,
					Settings: []types.AgentAccessSetting{
						{AgentAction: types.AgentActionComputerVision, Permission: types.PermissionEnabled},
					},
				},
			})
			require.NoError(t, err)
			cfg := created.Stack.AgentAccessConfig
			require.NotNil(t, cfg)
			assert.Equal(t, types.UserControlModeViewOnly, cfg.UserControlMode)
			require.Len(t, cfg.Settings, 1)

			upd, err := c.UpdateStack(t.Context(), &appstreamsdk.UpdateStackInput{
				Name:              aws.String("agent-stack"),
				AgentAccessConfig: &types.AgentAccessConfigForUpdate{UserControlMode: types.UserControlModeDisabled},
			})
			require.NoError(t, err)
			cfg = upd.Stack.AgentAccessConfig
			require.NotNil(t, cfg)
			assert.Equal(t, types.UserControlModeDisabled, cfg.UserControlMode)
			assert.Equal(t, types.ScreenImageFormatPng, cfg.ScreenImageFormat, "omitted member keeps its value")
			require.Len(t, cfg.Settings, 1)

			del, err := c.UpdateStack(t.Context(), &appstreamsdk.UpdateStackInput{
				Name:               aws.String("agent-stack"),
				AttributesToDelete: []types.StackAttribute{types.StackAttributeAgentAccessConfig},
			})
			require.NoError(t, err)
			assert.Nil(t, del.Stack.AgentAccessConfig)
		}},
		{name: "screenshots_need_bucket", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{
				Name: aws.String("agent-stack2"),
				AgentAccessConfig: &types.AgentAccessConfig{
					ScreenImageFormat:        types.ScreenImageFormatJpeg,
					ScreenResolution:         types.ScreenResolutionW1280xH720,
					ScreenshotsUploadEnabled: aws.Bool(true),
					Settings: []types.AgentAccessSetting{
						{AgentAction: types.AgentActionComputerInput, Permission: types.PermissionDisabled},
					},
				},
			})
			require.Error(t, err)
		}},
		{name: "bad_enum", run: func(t *testing.T, c *appstreamsdk.Client) {
			t.Helper()

			_, err := c.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{
				Name: aws.String("agent-stack3"),
				AgentAccessConfig: &types.AgentAccessConfig{
					ScreenImageFormat: "GIF",
					ScreenResolution:  types.ScreenResolutionW1280xH720,
					Settings: []types.AgentAccessSetting{
						{AgentAction: types.AgentActionComputerInput, Permission: types.PermissionDisabled},
					},
				},
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newMembersClient(t))
		})
	}
}

func TestRealClient_UpdateApplicationAttributesAndIcon(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)

	_, err := c.CreateApplication(t.Context(), &appstreamsdk.CreateApplicationInput{
		Name:             aws.String("app-x"),
		LaunchPath:       aws.String("C:\\app.exe"),
		AppBlockArn:      aws.String("arn:aws:appstream:us-east-1:000000000000:app-block/one"),
		Platforms:        []types.PlatformType{types.PlatformTypeWindows},
		InstanceFamilies: []string{"GENERAL_PURPOSE"},
		IconS3Location:   &types.S3Location{S3Bucket: aws.String("icons"), S3Key: aws.String("a.png")},
		LaunchParameters: aws.String("--x"),
		WorkingDirectory: aws.String("C:\\w"),
	})
	require.NoError(t, err)

	upd, err := c.UpdateApplication(t.Context(), &appstreamsdk.UpdateApplicationInput{
		Name:               aws.String("app-x"),
		AppBlockArn:        aws.String("arn:aws:appstream:us-east-1:000000000000:app-block/two"),
		IconS3Location:     &types.S3Location{S3Bucket: aws.String("icons"), S3Key: aws.String("b.png")},
		AttributesToDelete: []types.ApplicationAttribute{types.ApplicationAttributeLaunchParameters},
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:appstream:us-east-1:000000000000:app-block/two", aws.ToString(upd.Application.AppBlockArn))
	assert.Equal(t, "b.png", aws.ToString(upd.Application.IconS3Location.S3Key))
	assert.Empty(t, aws.ToString(upd.Application.LaunchParameters))
	assert.Equal(t, "C:\\w", aws.ToString(upd.Application.WorkingDirectory))
}

func TestRealClient_UpdateFleetDeleteVpcConfig(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)

	created, err := c.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
		Name:            aws.String("fleet-vpc"),
		InstanceType:    aws.String("stream.standard.medium"),
		ImageName:       aws.String("img"),
		ComputeCapacity: &types.ComputeCapacity{DesiredInstances: aws.Int32(1)},
		VpcConfig:       vpc(),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Fleet.VpcConfig)

	upd, err := c.UpdateFleet(t.Context(), &appstreamsdk.UpdateFleetInput{
		Name:            aws.String("fleet-vpc"),
		DeleteVpcConfig: aws.Bool(true), //nolint:staticcheck // deprecated SDK member under test.
	})
	require.NoError(t, err)

	if upd.Fleet.VpcConfig != nil {
		assert.Empty(t, upd.Fleet.VpcConfig.SubnetIds)
	}
}

func TestRealClient_CreateUserMessageAction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      appstreamsdk.CreateUserInput
		wantErr bool
	}{
		{name: "suppress", in: appstreamsdk.CreateUserInput{
			UserName: aws.String("a@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
			MessageAction: types.MessageActionSuppress, FirstName: aws.String("A"),
		}},
		{name: "bad_action", wantErr: true, in: appstreamsdk.CreateUserInput{
			UserName: aws.String("b@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
			MessageAction: "SHOUT",
		}},
		{name: "resend_with_names", wantErr: true, in: appstreamsdk.CreateUserInput{
			UserName: aws.String("c@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
			MessageAction: types.MessageActionResend, FirstName: aws.String("C"),
		}},
		{name: "resend_unknown_user", wantErr: true, in: appstreamsdk.CreateUserInput{
			UserName: aws.String("d@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
			MessageAction: types.MessageActionResend,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.in
			_, err := newMembersClient(t).CreateUser(t.Context(), &in)
			assert.Equal(t, tc.wantErr, err != nil)
		})
	}

	t.Run("resend_existing_user", func(t *testing.T) {
		t.Parallel()

		c := newMembersClient(t)
		_, err := c.CreateUser(t.Context(), &appstreamsdk.CreateUserInput{
			UserName: aws.String("e@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)

		_, err = c.CreateUser(t.Context(), &appstreamsdk.CreateUserInput{
			UserName: aws.String("e@example.com"), AuthenticationType: types.AuthenticationTypeUserpool,
			MessageAction: types.MessageActionResend,
		})
		require.NoError(t, err)
	})
}

func TestRealClient_DescribeSessionsInstanceID(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)

	_, err := c.CreateStack(t.Context(), &appstreamsdk.CreateStackInput{Name: aws.String("si-stack")})
	require.NoError(t, err)

	_, err = c.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
		Name:            aws.String("si-fleet"),
		InstanceType:    aws.String("stream.standard.medium"),
		ImageName:       aws.String("img"),
		ComputeCapacity: &types.ComputeCapacity{DesiredInstances: aws.Int32(1)},
	})
	require.NoError(t, err)

	for _, user := range []string{"u1", "u2"} {
		_, err = c.CreateStreamingURL(t.Context(), &appstreamsdk.CreateStreamingURLInput{
			StackName: aws.String("si-stack"), FleetName: aws.String("si-fleet"), UserId: aws.String(user),
		})
		require.NoError(t, err)
	}

	all, err := c.DescribeSessions(t.Context(), &appstreamsdk.DescribeSessionsInput{
		StackName: aws.String("si-stack"), FleetName: aws.String("si-fleet"),
	})
	require.NoError(t, err)
	require.Len(t, all.Sessions, 2)
	require.NotEmpty(t, aws.ToString(all.Sessions[0].InstanceId))
	assert.NotEqual(t, aws.ToString(all.Sessions[0].InstanceId), aws.ToString(all.Sessions[1].InstanceId))

	one, err := c.DescribeSessions(t.Context(), &appstreamsdk.DescribeSessionsInput{
		StackName: aws.String("si-stack"), FleetName: aws.String("si-fleet"),
		InstanceId: all.Sessions[1].InstanceId,
	})
	require.NoError(t, err)
	require.Len(t, one.Sessions, 1)
	assert.Equal(t, aws.ToString(all.Sessions[1].Id), aws.ToString(one.Sessions[0].Id))
}
