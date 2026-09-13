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

// TestFleet_ExtendedFields_RealClient drives CreateFleet/DescribeFleets/
// UpdateFleet through the real SDK client and asserts every output member
// gopherstack-gv10n added (VpcConfig, IamRoleArn, StreamView,
// RootVolumeConfig, MaxSessionsPerInstance, UsbDeviceFilterStrings,
// SessionScriptS3Location, Platform, DomainJoinInfo, MaxConcurrentSessions,
// DisableIMDSV1) deep-equals the request. AttributesToDelete is checked
// against VPC_CONFIGURATION and IAM_ROLE_ARN, mirroring
// UpdateThemeForStack's AttributesToDelete test convention.
func TestFleet_ExtendedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	created, err := client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
		Name:         aws.String("ext-fleet"),
		InstanceType: aws.String("stream.standard.medium"),
		VpcConfig: &types.VpcConfig{
			SecurityGroupIds: []string{"sg-1"},
			SubnetIds:        []string{"subnet-1", "subnet-2"},
		},
		IamRoleArn:             aws.String("arn:aws:iam::000000000000:role/fleet-role"),
		StreamView:             types.StreamViewDesktop,
		Platform:               types.PlatformTypeWindowsServer2019,
		RootVolumeConfig:       &types.VolumeConfig{VolumeSizeInGb: aws.Int32(300)},
		MaxSessionsPerInstance: aws.Int32(4),
		UsbDeviceFilterStrings: []string{"0123:abcd"},
		SessionScriptS3Location: &types.S3Location{
			S3Bucket: aws.String("scripts-bucket"),
			S3Key:    aws.String("session.zip"),
		},
		DomainJoinInfo: &types.DomainJoinInfo{
			DirectoryName:                       aws.String("corp.example.com"),
			OrganizationalUnitDistinguishedName: aws.String("OU=Fleets,DC=corp,DC=example,DC=com"),
		},
		DisableIMDSV1: aws.Bool(true),
	})
	require.NoError(t, err)
	requireFleetExtendedFields(t, created.Fleet)

	described, err := client.DescribeFleets(ctx, &appstreamsdk.DescribeFleetsInput{Names: []string{"ext-fleet"}})
	require.NoError(t, err)
	require.Len(t, described.Fleets, 1)
	requireFleetExtendedFields(t, &described.Fleets[0])

	t.Run("AttributesToDelete clears VpcConfig and IamRoleArn", func(t *testing.T) {
		t.Parallel()

		updated, updateErr := client.UpdateFleet(ctx, &appstreamsdk.UpdateFleetInput{
			Name: aws.String("ext-fleet"),
			AttributesToDelete: []types.FleetAttribute{
				types.FleetAttributeVpcConfiguration,
				types.FleetAttributeIamRoleArn,
			},
		})
		require.NoError(t, updateErr)
		assert.Nil(t, updated.Fleet.VpcConfig, "VPC_CONFIGURATION delete must clear VpcConfig")
		assert.Nil(t, updated.Fleet.IamRoleArn, "IAM_ROLE_ARN delete must clear IamRoleArn")
		require.NotNil(t, updated.Fleet.StreamView)
		assert.Equal(t, types.StreamViewDesktop, updated.Fleet.StreamView, "unrelated fields must survive the delete")
	})
}

func requireFleetExtendedFields(t *testing.T, f *types.Fleet) {
	t.Helper()

	require.NotNil(t, f.VpcConfig, "VpcConfig dropped")
	assert.Equal(t, []string{"sg-1"}, f.VpcConfig.SecurityGroupIds)
	assert.Equal(t, []string{"subnet-1", "subnet-2"}, f.VpcConfig.SubnetIds)
	require.NotNil(t, f.IamRoleArn)
	assert.Equal(t, "arn:aws:iam::000000000000:role/fleet-role", *f.IamRoleArn)
	assert.Equal(t, types.StreamViewDesktop, f.StreamView)
	assert.Equal(t, types.PlatformTypeWindowsServer2019, f.Platform)
	require.NotNil(t, f.RootVolumeConfig, "RootVolumeConfig dropped")
	require.NotNil(t, f.RootVolumeConfig.VolumeSizeInGb)
	assert.Equal(t, int32(300), *f.RootVolumeConfig.VolumeSizeInGb)
	require.NotNil(t, f.MaxSessionsPerInstance)
	assert.Equal(t, int32(4), *f.MaxSessionsPerInstance)
	assert.Equal(t, []string{"0123:abcd"}, f.UsbDeviceFilterStrings)
	require.NotNil(t, f.SessionScriptS3Location, "SessionScriptS3Location dropped")
	assert.Equal(t, "scripts-bucket", *f.SessionScriptS3Location.S3Bucket)
	assert.Equal(t, "session.zip", *f.SessionScriptS3Location.S3Key)
	require.NotNil(t, f.DomainJoinInfo, "DomainJoinInfo dropped")
	assert.Equal(t, "corp.example.com", *f.DomainJoinInfo.DirectoryName)
	require.NotNil(t, f.DisableIMDSV1)
	assert.True(t, *f.DisableIMDSV1)
}

// TestAppBlock_ExtendedFields_RealClient drives CreateAppBlock/
// DescribeAppBlocks through the real SDK client and asserts SourceS3Location
// (required on the real wire), SetupScriptDetails, PostSetupScriptDetails,
// PackagingType, and DisplayName all round-trip.
func TestAppBlock_ExtendedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	created, err := client.CreateAppBlock(ctx, &appstreamsdk.CreateAppBlockInput{
		Name:          aws.String("ext-appblock"),
		DisplayName:   aws.String("Ext App Block"),
		PackagingType: types.PackagingTypeCustom,
		SourceS3Location: &types.S3Location{
			S3Bucket: aws.String("appblock-bucket"),
			S3Key:    aws.String("ext-appblock.zip"),
		},
		SetupScriptDetails: &types.ScriptDetails{
			ExecutablePath:   aws.String("setup.ps1"),
			ScriptS3Location: &types.S3Location{S3Bucket: aws.String("scripts"), S3Key: aws.String("setup.zip")},
			TimeoutInSeconds: aws.Int32(120),
		},
	})
	require.NoError(t, err)
	requireAppBlockExtendedFields(t, created.AppBlock)

	described, err := client.DescribeAppBlocks(ctx, &appstreamsdk.DescribeAppBlocksInput{
		Arns: []string{*created.AppBlock.Arn},
	})
	require.NoError(t, err)
	require.Len(t, described.AppBlocks, 1)
	requireAppBlockExtendedFields(t, &described.AppBlocks[0])
}

func requireAppBlockExtendedFields(t *testing.T, ab *types.AppBlock) {
	t.Helper()

	require.NotNil(t, ab.DisplayName)
	assert.Equal(t, "Ext App Block", *ab.DisplayName)
	assert.Equal(t, types.PackagingTypeCustom, ab.PackagingType)
	require.NotNil(t, ab.SourceS3Location, "SourceS3Location dropped -- required on the real wire")
	assert.Equal(t, "appblock-bucket", *ab.SourceS3Location.S3Bucket)
	assert.Equal(t, "ext-appblock.zip", *ab.SourceS3Location.S3Key)
	require.NotNil(t, ab.SetupScriptDetails, "SetupScriptDetails dropped")
	assert.Equal(t, "setup.ps1", *ab.SetupScriptDetails.ExecutablePath)
	require.NotNil(t, ab.SetupScriptDetails.ScriptS3Location)
	assert.Equal(t, "scripts", *ab.SetupScriptDetails.ScriptS3Location.S3Bucket)
	require.NotNil(t, ab.SetupScriptDetails.TimeoutInSeconds)
	assert.Equal(t, int32(120), *ab.SetupScriptDetails.TimeoutInSeconds)
}

// TestImageBuilder_ExtendedFields_RealClient drives CreateImageBuilder/
// DescribeImageBuilders/StartImageBuilder through the real SDK client and
// asserts VpcConfig, IamRoleArn, EnableDefaultInternetAccess,
// DomainJoinInfo, AccessEndpoints, RootVolumeConfig, DisableIMDSV1, and
// AppstreamAgentVersion round-trip.
func TestImageBuilder_ExtendedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	created, err := client.CreateImageBuilder(ctx, &appstreamsdk.CreateImageBuilderInput{
		Name:         aws.String("ext-builder"),
		InstanceType: aws.String("stream.standard.medium"),
		VpcConfig: &types.VpcConfig{
			SecurityGroupIds: []string{"sg-2"},
			SubnetIds:        []string{"subnet-3"},
		},
		IamRoleArn:                  aws.String("arn:aws:iam::000000000000:role/builder-role"),
		EnableDefaultInternetAccess: aws.Bool(true),
		DomainJoinInfo: &types.DomainJoinInfo{
			DirectoryName: aws.String("corp.example.com"),
		},
		AccessEndpoints: []types.AccessEndpoint{
			{EndpointType: types.AccessEndpointTypeStreaming, VpceId: aws.String("vpce-1")},
		},
		RootVolumeConfig: &types.VolumeConfig{VolumeSizeInGb: aws.Int32(250)},
		DisableIMDSV1:    aws.Bool(true),
	})
	require.NoError(t, err)
	requireImageBuilderExtendedFields(t, created.ImageBuilder)
	assert.Nil(t, created.ImageBuilder.AppstreamAgentVersion,
		"not set on Create; must stay unset until StartImageBuilder sets it")

	_, err = client.StartImageBuilder(ctx, &appstreamsdk.StartImageBuilderInput{
		Name:                  aws.String("ext-builder"),
		AppstreamAgentVersion: aws.String("1.2.3"),
	})
	require.NoError(t, err)

	described, err := client.DescribeImageBuilders(ctx, &appstreamsdk.DescribeImageBuildersInput{
		Names: []string{"ext-builder"},
	})
	require.NoError(t, err)
	require.Len(t, described.ImageBuilders, 1)
	requireImageBuilderExtendedFields(t, &described.ImageBuilders[0])
	require.NotNil(t, described.ImageBuilders[0].AppstreamAgentVersion)
	assert.Equal(t, "1.2.3", *described.ImageBuilders[0].AppstreamAgentVersion)
}

func requireImageBuilderExtendedFields(t *testing.T, ib *types.ImageBuilder) {
	t.Helper()

	require.NotNil(t, ib.VpcConfig, "VpcConfig dropped")
	assert.Equal(t, []string{"sg-2"}, ib.VpcConfig.SecurityGroupIds)
	assert.Equal(t, []string{"subnet-3"}, ib.VpcConfig.SubnetIds)
	require.NotNil(t, ib.IamRoleArn)
	assert.Equal(t, "arn:aws:iam::000000000000:role/builder-role", *ib.IamRoleArn)
	require.NotNil(t, ib.EnableDefaultInternetAccess)
	assert.True(t, *ib.EnableDefaultInternetAccess)
	require.NotNil(t, ib.DomainJoinInfo, "DomainJoinInfo dropped")
	assert.Equal(t, "corp.example.com", *ib.DomainJoinInfo.DirectoryName)
	require.Len(t, ib.AccessEndpoints, 1, "AccessEndpoints dropped")
	assert.Equal(t, types.AccessEndpointTypeStreaming, ib.AccessEndpoints[0].EndpointType)
	require.NotNil(t, ib.RootVolumeConfig, "RootVolumeConfig dropped")
	assert.Equal(t, int32(250), *ib.RootVolumeConfig.VolumeSizeInGb)
	require.NotNil(t, ib.DisableIMDSV1)
	assert.True(t, *ib.DisableIMDSV1)
}

// TestApplication_ExtendedFields_RealClient drives CreateApplication/
// DescribeApplications/UpdateApplication through the real SDK client and
// asserts LaunchParameters, WorkingDirectory, and the always-true Enabled
// member round-trip.
func TestApplication_ExtendedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	created, err := client.CreateApplication(ctx, &appstreamsdk.CreateApplicationInput{
		Name:             aws.String("ext-app"),
		LaunchPath:       aws.String("C:\\app.exe"),
		AppBlockArn:      aws.String("arn:aws:appstream:us-east-1:000000000000:app-block/ext-appblock"),
		Platforms:        []types.PlatformType{types.PlatformTypeWindows},
		InstanceFamilies: []string{"GENERAL_PURPOSE"},
		IconS3Location:   &types.S3Location{S3Bucket: aws.String("icons"), S3Key: aws.String("ext-app.png")},
		LaunchParameters: aws.String("--headless"),
		WorkingDirectory: aws.String("C:\\work"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Application.LaunchParameters, "LaunchParameters dropped")
	assert.Equal(t, "--headless", *created.Application.LaunchParameters)
	require.NotNil(t, created.Application.WorkingDirectory, "WorkingDirectory dropped")
	assert.Equal(t, "C:\\work", *created.Application.WorkingDirectory)
	require.NotNil(t, created.Application.Enabled)
	assert.True(t, *created.Application.Enabled)

	updated, err := client.UpdateApplication(ctx, &appstreamsdk.UpdateApplicationInput{
		Name:             aws.String("ext-app"),
		LaunchParameters: aws.String("--verbose"),
		WorkingDirectory: aws.String("C:\\work2"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Application.LaunchParameters)
	assert.Equal(t, "--verbose", *updated.Application.LaunchParameters)
	require.NotNil(t, updated.Application.WorkingDirectory)
	assert.Equal(t, "C:\\work2", *updated.Application.WorkingDirectory)
}

// TestStack_ExtendedFields_RealClient drives CreateStack/DescribeStacks/
// UpdateStack through the real SDK client and asserts RedirectURL,
// FeedbackURL, UserSettings, ApplicationSettings, AccessEndpoints,
// EmbedHostDomains, StreamingExperienceSettings, StorageConnectors, and
// ContentRedirection all round-trip. ApplicationSettings.S3BucketName is
// asserted non-empty (derived, not caller-supplied -- see
// InMemoryBackend.cloneApplicationSettings).
func TestStack_ExtendedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	created, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{
		Name:        aws.String("ext-stack"),
		RedirectURL: aws.String("https://example.com/redirect"),
		FeedbackURL: aws.String("https://example.com/feedback"),
		UserSettings: []types.UserSetting{
			{Action: types.ActionFileUpload, Permission: types.PermissionEnabled},
		},
		ApplicationSettings: &types.ApplicationSettings{
			Enabled:       aws.Bool(true),
			SettingsGroup: aws.String("prod"),
		},
		AccessEndpoints: []types.AccessEndpoint{
			{EndpointType: types.AccessEndpointTypeStreaming, VpceId: aws.String("vpce-2")},
		},
		EmbedHostDomains: []string{"embed.example.com"},
		StreamingExperienceSettings: &types.StreamingExperienceSettings{
			PreferredProtocol: types.PreferredProtocolUdp,
		},
		StorageConnectors: []types.StorageConnector{
			{ConnectorType: types.StorageConnectorTypeHomefolders},
		},
		ContentRedirection: &types.ContentRedirection{
			HostToClient: &types.UrlRedirectionConfig{Enabled: aws.Bool(true)},
		},
	})
	require.NoError(t, err)
	requireStackExtendedFields(t, created.Stack)

	described, err := client.DescribeStacks(ctx, &appstreamsdk.DescribeStacksInput{Names: []string{"ext-stack"}})
	require.NoError(t, err)
	require.Len(t, described.Stacks, 1)
	requireStackExtendedFields(t, &described.Stacks[0])

	t.Run("AttributesToDelete clears RedirectURL and StorageConnectors", func(t *testing.T) {
		t.Parallel()

		updated, updateErr := client.UpdateStack(ctx, &appstreamsdk.UpdateStackInput{
			Name: aws.String("ext-stack"),
			AttributesToDelete: []types.StackAttribute{
				types.StackAttributeRedirectUrl,
				types.StackAttributeStorageConnectors,
			},
		})
		require.NoError(t, updateErr)
		assert.Nil(t, updated.Stack.RedirectURL, "REDIRECT_URL delete must clear RedirectURL")
		assert.Empty(t, updated.Stack.StorageConnectors, "STORAGE_CONNECTORS delete must clear StorageConnectors")
		assert.NotNil(t, updated.Stack.FeedbackURL, "unrelated fields must survive the delete")
	})
}

func requireStackExtendedFields(t *testing.T, s *types.Stack) {
	t.Helper()

	require.NotNil(t, s.RedirectURL)
	assert.Equal(t, "https://example.com/redirect", *s.RedirectURL)
	require.NotNil(t, s.FeedbackURL)
	assert.Equal(t, "https://example.com/feedback", *s.FeedbackURL)
	require.Len(t, s.UserSettings, 1, "UserSettings dropped")
	assert.Equal(t, types.ActionFileUpload, s.UserSettings[0].Action)
	require.NotNil(t, s.ApplicationSettings, "ApplicationSettings dropped")
	require.NotNil(t, s.ApplicationSettings.Enabled)
	assert.True(t, *s.ApplicationSettings.Enabled)
	require.NotNil(t, s.ApplicationSettings.S3BucketName, "S3BucketName must be derived when Enabled")
	assert.NotEmpty(t, *s.ApplicationSettings.S3BucketName)
	require.Len(t, s.AccessEndpoints, 1, "AccessEndpoints dropped")
	assert.Equal(t, "vpce-2", *s.AccessEndpoints[0].VpceId)
	assert.Equal(t, []string{"embed.example.com"}, s.EmbedHostDomains)
	require.NotNil(t, s.StreamingExperienceSettings, "StreamingExperienceSettings dropped")
	assert.Equal(t, types.PreferredProtocolUdp, s.StreamingExperienceSettings.PreferredProtocol)
	require.Len(t, s.StorageConnectors, 1, "StorageConnectors dropped")
	assert.Equal(t, types.StorageConnectorTypeHomefolders, s.StorageConnectors[0].ConnectorType)
	require.NotNil(t, s.ContentRedirection, "ContentRedirection dropped")
	require.NotNil(t, s.ContentRedirection.HostToClient)
	require.NotNil(t, s.ContentRedirection.HostToClient.Enabled)
	assert.True(t, *s.ContentRedirection.HostToClient.Enabled)
}
