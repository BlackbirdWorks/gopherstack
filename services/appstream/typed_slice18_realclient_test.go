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

// newSlice18AppStreamClient stands up a fresh backend/handler/client triple
// for gopherstack-n3zi typed slice 18. AppStream is rpc-v2-cbor
// (appstream@v1.64.5 has only a smithy CBOR serializer, no JSON fallback), so
// the real client is the only oracle for its encoding.
func newSlice18AppStreamClient(t *testing.T) *appstreamsdk.Client {
	t.Helper()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	h := appstream.NewHandler(backend)

	return newTestAppStreamClient(t, h)
}

// TestTypedSlice18AppStream_RealClient drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage slice 18).
func TestTypedSlice18AppStream_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("fleets_and_associations", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-fleet-stack")})
		require.NoError(t, err)

		_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
			Name:         aws.String("slice18-fleet"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
			ComputeCapacity: &types.ComputeCapacity{
				DesiredInstances: aws.Int32(1),
			},
		})
		require.NoError(t, err)

		_, err = client.AssociateFleet(ctx, &appstreamsdk.AssociateFleetInput{
			FleetName: aws.String("slice18-fleet"),
			StackName: aws.String("slice18-fleet-stack"),
		})
		require.NoError(t, err)

		_, err = client.StartFleet(ctx, &appstreamsdk.StartFleetInput{Name: aws.String("slice18-fleet")})
		require.NoError(t, err)

		listedStacks, err := client.ListAssociatedStacks(
			ctx,
			&appstreamsdk.ListAssociatedStacksInput{FleetName: aws.String("slice18-fleet")},
		)
		require.NoError(t, err)
		assert.Contains(t, listedStacks.Names, "slice18-fleet-stack")

		listedFleets, err := client.ListAssociatedFleets(
			ctx,
			&appstreamsdk.ListAssociatedFleetsInput{StackName: aws.String("slice18-fleet-stack")},
		)
		require.NoError(t, err)
		assert.Contains(t, listedFleets.Names, "slice18-fleet")

		_, err = client.StopFleet(ctx, &appstreamsdk.StopFleetInput{Name: aws.String("slice18-fleet")})
		require.NoError(t, err)

		_, err = client.DisassociateFleet(ctx, &appstreamsdk.DisassociateFleetInput{
			FleetName: aws.String("slice18-fleet"),
			StackName: aws.String("slice18-fleet-stack"),
		})
		require.NoError(t, err)

		afterDisassoc, err := client.ListAssociatedStacks(
			ctx,
			&appstreamsdk.ListAssociatedStacksInput{FleetName: aws.String("slice18-fleet")},
		)
		require.NoError(t, err)
		assert.Empty(t, afterDisassoc.Names)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		created, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-tag-stack")})
		require.NoError(t, err)
		stackARN := aws.ToString(created.Stack.Arn)

		_, err = client.TagResource(ctx, &appstreamsdk.TagResourceInput{
			ResourceArn: aws.String(stackARN),
			Tags:        map[string]string{"owner": "slice18"},
		})
		require.NoError(t, err)

		listed, err := client.ListTagsForResource(ctx, &appstreamsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(stackARN),
		})
		require.NoError(t, err)
		assert.Equal(t, "slice18", listed.Tags["owner"])

		_, err = client.UntagResource(ctx, &appstreamsdk.UntagResourceInput{
			ResourceArn: aws.String(stackARN),
			TagKeys:     []string{"owner"},
		})
		require.NoError(t, err)

		afterUntag, err := client.ListTagsForResource(ctx, &appstreamsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(stackARN),
		})
		require.NoError(t, err)
		assert.Empty(t, afterUntag.Tags)
	})

	t.Run("app_blocks_and_builders", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		createdBlock, err := client.CreateAppBlock(ctx, &appstreamsdk.CreateAppBlockInput{
			Name: aws.String("slice18-appblock"),
			SourceS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-bucket"),
				S3Key:    aws.String("appblock.zip"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createdBlock.AppBlock)

		_, err = client.DeleteAppBlock(ctx, &appstreamsdk.DeleteAppBlockInput{Name: aws.String("slice18-appblock")})
		require.NoError(t, err)

		createdBlock2, err := client.CreateAppBlock(ctx, &appstreamsdk.CreateAppBlockInput{
			Name: aws.String("slice18-appblock2"),
			SourceS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-bucket"),
				S3Key:    aws.String("appblock2.zip"),
			},
		})
		require.NoError(t, err)
		appBlockARN := aws.ToString(createdBlock2.AppBlock.Arn)

		createdBuilder, err := client.CreateAppBlockBuilder(ctx, &appstreamsdk.CreateAppBlockBuilderInput{
			Name:         aws.String("slice18-builder"),
			InstanceType: aws.String("stream.standard.medium"),
			Platform:     types.AppBlockBuilderPlatformTypeWindowsServer2019,
			VpcConfig: &types.VpcConfig{
				SecurityGroupIds: []string{"sg-1"},
				SubnetIds:        []string{"subnet-1"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createdBuilder.AppBlockBuilder)

		started, err := client.StartAppBlockBuilder(ctx, &appstreamsdk.StartAppBlockBuilderInput{
			Name: aws.String("slice18-builder"),
		})
		require.NoError(t, err)
		require.NotNil(t, started.AppBlockBuilder)
		assert.Equal(t, types.AppBlockBuilderStateRunning, started.AppBlockBuilder.State)

		stopped, err := client.StopAppBlockBuilder(ctx, &appstreamsdk.StopAppBlockBuilderInput{
			Name: aws.String("slice18-builder"),
		})
		require.NoError(t, err)
		require.NotNil(t, stopped.AppBlockBuilder)
		assert.Equal(t, types.AppBlockBuilderStateStopped, stopped.AppBlockBuilder.State)

		url, err := client.CreateAppBlockBuilderStreamingURL(
			ctx,
			&appstreamsdk.CreateAppBlockBuilderStreamingURLInput{AppBlockBuilderName: aws.String("slice18-builder")},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(url.StreamingURL))

		_, err = client.AssociateAppBlockBuilderAppBlock(ctx, &appstreamsdk.AssociateAppBlockBuilderAppBlockInput{
			AppBlockBuilderName: aws.String("slice18-builder"),
			AppBlockArn:         aws.String(appBlockARN),
		})
		require.NoError(t, err)

		assocs, err := client.DescribeAppBlockBuilderAppBlockAssociations(
			ctx,
			&appstreamsdk.DescribeAppBlockBuilderAppBlockAssociationsInput{
				AppBlockBuilderName: aws.String("slice18-builder"),
			},
		)
		require.NoError(t, err)
		require.Len(t, assocs.AppBlockBuilderAppBlockAssociations, 1)
		assert.Equal(t, appBlockARN, aws.ToString(assocs.AppBlockBuilderAppBlockAssociations[0].AppBlockArn))

		_, err = client.DisassociateAppBlockBuilderAppBlock(ctx, &appstreamsdk.DisassociateAppBlockBuilderAppBlockInput{
			AppBlockBuilderName: aws.String("slice18-builder"),
			AppBlockArn:         aws.String(appBlockARN),
		})
		require.NoError(t, err)

		_, err = client.DeleteAppBlockBuilder(ctx, &appstreamsdk.DeleteAppBlockBuilderInput{
			Name: aws.String("slice18-builder"),
		})
		require.NoError(t, err)
	})

	t.Run("applications", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		createdBlock, err := client.CreateAppBlock(ctx, &appstreamsdk.CreateAppBlockInput{
			Name: aws.String("slice18-app-appblock"),
			SourceS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-bucket"),
				S3Key:    aws.String("appblock.zip"),
			},
		})
		require.NoError(t, err)

		created, err := client.CreateApplication(ctx, &appstreamsdk.CreateApplicationInput{
			Name:             aws.String("slice18-app"),
			LaunchPath:       aws.String("C:\\app.exe"),
			Platforms:        []types.PlatformType{types.PlatformTypeWindowsServer2019},
			AppBlockArn:      createdBlock.AppBlock.Arn,
			InstanceFamilies: []string{"GENERAL_PURPOSE"},
			IconS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-icons"),
				S3Key:    aws.String("app.png"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, created.Application)

		listed, err := client.DescribeApplications(ctx, &appstreamsdk.DescribeApplicationsInput{
			Arns: []string{aws.ToString(created.Application.Arn)},
		})
		require.NoError(t, err)
		require.Len(t, listed.Applications, 1)
		assert.Equal(t, "slice18-app", aws.ToString(listed.Applications[0].Name))

		// This backend always reports zero license-usage records (no license
		// tracking model exists). An empty CBOR list decodes to a nil Go
		// slice here (the SDK's deserializer never allocates one when zero
		// elements are read), so only the error is asserted -- not
		// non-nilness, which would fail identically against a real,
		// legitimately-empty AWS response.
		_, err = client.DescribeAppLicenseUsage(ctx, &appstreamsdk.DescribeAppLicenseUsageInput{
			BillingPeriod: aws.String("2026-09"),
		})
		require.NoError(t, err)

		_, err = client.DeleteApplication(ctx, &appstreamsdk.DeleteApplicationInput{Name: aws.String("slice18-app")})
		require.NoError(t, err)
	})

	t.Run("application_fleet_associations", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		createdBlock, err := client.CreateAppBlock(ctx, &appstreamsdk.CreateAppBlockInput{
			Name: aws.String("slice18-af-appblock"),
			SourceS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-bucket"),
				S3Key:    aws.String("appblock.zip"),
			},
		})
		require.NoError(t, err)

		createdApp, err := client.CreateApplication(ctx, &appstreamsdk.CreateApplicationInput{
			Name:             aws.String("slice18-af-app"),
			LaunchPath:       aws.String("C:\\app.exe"),
			Platforms:        []types.PlatformType{types.PlatformTypeWindowsServer2019},
			AppBlockArn:      createdBlock.AppBlock.Arn,
			InstanceFamilies: []string{"GENERAL_PURPOSE"},
			IconS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-icons"),
				S3Key:    aws.String("app.png"),
			},
		})
		require.NoError(t, err)
		appARN := aws.ToString(createdApp.Application.Arn)

		_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
			Name:         aws.String("slice18-af-fleet"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
			ComputeCapacity: &types.ComputeCapacity{
				DesiredInstances: aws.Int32(1),
			},
		})
		require.NoError(t, err)

		_, err = client.AssociateApplicationFleet(ctx, &appstreamsdk.AssociateApplicationFleetInput{
			ApplicationArn: aws.String(appARN),
			FleetName:      aws.String("slice18-af-fleet"),
		})
		require.NoError(t, err)

		assocs, err := client.DescribeApplicationFleetAssociations(
			ctx,
			&appstreamsdk.DescribeApplicationFleetAssociationsInput{
				ApplicationArn: aws.String(appARN),
			},
		)
		require.NoError(t, err)
		require.Len(t, assocs.ApplicationFleetAssociations, 1)
		assert.Equal(t, "slice18-af-fleet", aws.ToString(assocs.ApplicationFleetAssociations[0].FleetName))

		_, err = client.DisassociateApplicationFleet(ctx, &appstreamsdk.DisassociateApplicationFleetInput{
			ApplicationArn: aws.String(appARN),
			FleetName:      aws.String("slice18-af-fleet"),
		})
		require.NoError(t, err)

		afterDisassoc, err := client.DescribeApplicationFleetAssociations(
			ctx,
			&appstreamsdk.DescribeApplicationFleetAssociationsInput{
				ApplicationArn: aws.String(appARN),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, afterDisassoc.ApplicationFleetAssociations)
	})

	t.Run("entitlements", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-ent-stack")})
		require.NoError(t, err)

		_, err = client.CreateEntitlement(ctx, &appstreamsdk.CreateEntitlementInput{
			Name:          aws.String("slice18-entitlement"),
			StackName:     aws.String("slice18-ent-stack"),
			AppVisibility: types.AppVisibilityAll,
			Attributes: []types.EntitlementAttribute{
				{Name: aws.String("roles"), Value: aws.String("admin")},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeEntitlements(ctx, &appstreamsdk.DescribeEntitlementsInput{
			StackName: aws.String("slice18-ent-stack"),
			Name:      aws.String("slice18-entitlement"),
		})
		require.NoError(t, err)
		require.Len(t, desc.Entitlements, 1)

		updated, err := client.UpdateEntitlement(ctx, &appstreamsdk.UpdateEntitlementInput{
			Name:          aws.String("slice18-entitlement"),
			StackName:     aws.String("slice18-ent-stack"),
			AppVisibility: types.AppVisibilityAssociated,
		})
		require.NoError(t, err)
		require.NotNil(t, updated.Entitlement)
		assert.Equal(t, types.AppVisibilityAssociated, updated.Entitlement.AppVisibility)

		_, err = client.AssociateApplicationToEntitlement(ctx, &appstreamsdk.AssociateApplicationToEntitlementInput{
			ApplicationIdentifier: aws.String("slice18-ent-app"),
			EntitlementName:       aws.String("slice18-entitlement"),
			StackName:             aws.String("slice18-ent-stack"),
		})
		require.NoError(t, err)

		entitled, err := client.ListEntitledApplications(ctx, &appstreamsdk.ListEntitledApplicationsInput{
			EntitlementName: aws.String("slice18-entitlement"),
			StackName:       aws.String("slice18-ent-stack"),
		})
		require.NoError(t, err)
		require.Len(t, entitled.EntitledApplications, 1)
		assert.Equal(t, "slice18-ent-app", aws.ToString(entitled.EntitledApplications[0].ApplicationIdentifier))

		_, err = client.DisassociateApplicationFromEntitlement(
			ctx,
			&appstreamsdk.DisassociateApplicationFromEntitlementInput{
				ApplicationIdentifier: aws.String("slice18-ent-app"),
				EntitlementName:       aws.String("slice18-entitlement"),
				StackName:             aws.String("slice18-ent-stack"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteEntitlement(ctx, &appstreamsdk.DeleteEntitlementInput{
			Name:      aws.String("slice18-entitlement"),
			StackName: aws.String("slice18-ent-stack"),
		})
		require.NoError(t, err)
	})

	t.Run("directory_configs", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		created, err := client.CreateDirectoryConfig(ctx, &appstreamsdk.CreateDirectoryConfigInput{
			DirectoryName:                        aws.String("slice18.example.com"),
			OrganizationalUnitDistinguishedNames: []string{"OU=slice18,DC=example,DC=com"},
			ServiceAccountCredentials: &types.ServiceAccountCredentials{
				AccountName:     aws.String("svc-account"),
				AccountPassword: aws.String("super-secret"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, created.DirectoryConfig)

		listed, err := client.DescribeDirectoryConfigs(ctx, &appstreamsdk.DescribeDirectoryConfigsInput{
			DirectoryNames: []string{"slice18.example.com"},
		})
		require.NoError(t, err)
		require.Len(t, listed.DirectoryConfigs, 1)

		updated, err := client.UpdateDirectoryConfig(ctx, &appstreamsdk.UpdateDirectoryConfigInput{
			DirectoryName: aws.String("slice18.example.com"),
			ServiceAccountCredentials: &types.ServiceAccountCredentials{
				AccountName:     aws.String("svc-account-2"),
				AccountPassword: aws.String("super-secret-2"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, updated.DirectoryConfig)
		assert.Equal(t, "svc-account-2", aws.ToString(updated.DirectoryConfig.ServiceAccountCredentials.AccountName))

		_, err = client.DeleteDirectoryConfig(ctx, &appstreamsdk.DeleteDirectoryConfigInput{
			DirectoryName: aws.String("slice18.example.com"),
		})
		require.NoError(t, err)
	})

	t.Run("images", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateImportedImage(ctx, &appstreamsdk.CreateImportedImageInput{
			Name: aws.String("slice18-source-image"),
		})
		require.NoError(t, err)

		copied, err := client.CopyImage(ctx, &appstreamsdk.CopyImageInput{
			SourceImageName:      aws.String("slice18-source-image"),
			DestinationImageName: aws.String("slice18-copied-image"),
			DestinationRegion:    aws.String("us-west-2"),
		})
		require.NoError(t, err)
		assert.Equal(t, "slice18-copied-image", aws.ToString(copied.DestinationImageName))

		_, err = client.UpdateImagePermissions(ctx, &appstreamsdk.UpdateImagePermissionsInput{
			Name:            aws.String("slice18-source-image"),
			SharedAccountId: aws.String("111111111111"),
			ImagePermissions: &types.ImagePermissions{
				AllowFleet: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteImagePermissions(ctx, &appstreamsdk.DeleteImagePermissionsInput{
			Name:            aws.String("slice18-source-image"),
			SharedAccountId: aws.String("111111111111"),
		})
		require.NoError(t, err)

		exportTask, err := client.CreateExportImageTask(ctx, &appstreamsdk.CreateExportImageTaskInput{
			ImageName:      aws.String("slice18-source-image"),
			AmiName:        aws.String("slice18-ami"),
			AmiDescription: aws.String("slice18 export"),
			IamRoleArn:     aws.String("arn:aws:iam::000000000000:role/export-role"),
		})
		require.NoError(t, err)
		require.NotNil(t, exportTask.ExportImageTask)
		taskID := aws.ToString(exportTask.ExportImageTask.TaskId)

		gotTask, err := client.GetExportImageTask(ctx, &appstreamsdk.GetExportImageTaskInput{
			TaskId: aws.String(taskID),
		})
		require.NoError(t, err)
		require.NotNil(t, gotTask.ExportImageTask)
		assert.Equal(t, taskID, aws.ToString(gotTask.ExportImageTask.TaskId))

		listed, err := client.ListExportImageTasks(ctx, &appstreamsdk.ListExportImageTasksInput{})
		require.NoError(t, err)

		var found bool

		for _, tk := range listed.ExportImageTasks {
			if aws.ToString(tk.TaskId) == taskID {
				found = true
			}
		}

		assert.True(t, found, "expected created export image task to appear in ListExportImageTasks")
	})

	t.Run("image_builders", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateImageBuilder(ctx, &appstreamsdk.CreateImageBuilderInput{
			Name:         aws.String("slice18-image-builder"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
		})
		require.NoError(t, err)

		stopped, err := client.StopImageBuilder(ctx, &appstreamsdk.StopImageBuilderInput{
			Name: aws.String("slice18-image-builder"),
		})
		require.NoError(t, err)
		require.NotNil(t, stopped.ImageBuilder)

		url, err := client.CreateImageBuilderStreamingURL(ctx, &appstreamsdk.CreateImageBuilderStreamingURLInput{
			Name: aws.String("slice18-image-builder"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(url.StreamingURL))

		_, err = client.AssociateSoftwareToImageBuilder(ctx, &appstreamsdk.AssociateSoftwareToImageBuilderInput{
			ImageBuilderName: aws.String("slice18-image-builder"),
			SoftwareNames:    []string{"slice18-tool"},
		})
		require.NoError(t, err)

		_, err = client.DisassociateSoftwareFromImageBuilder(
			ctx,
			&appstreamsdk.DisassociateSoftwareFromImageBuilderInput{
				ImageBuilderName: aws.String("slice18-image-builder"),
				SoftwareNames:    []string{"slice18-tool"},
			},
		)
		require.NoError(t, err)

		_, err = client.StartSoftwareDeploymentToImageBuilder(
			ctx,
			&appstreamsdk.StartSoftwareDeploymentToImageBuilderInput{
				ImageBuilderName: aws.String("slice18-image-builder"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("users", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateUser(ctx, &appstreamsdk.CreateUserInput{
			UserName:           aws.String("slice18-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)

		_, err = client.DisableUser(ctx, &appstreamsdk.DisableUserInput{
			UserName:           aws.String("slice18-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)

		_, err = client.EnableUser(ctx, &appstreamsdk.EnableUserInput{
			UserName:           aws.String("slice18-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)

		_, err = client.DeleteUser(ctx, &appstreamsdk.DeleteUserInput{
			UserName:           aws.String("slice18-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)
	})

	t.Run("user_stack_associations", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-usa-stack")})
		require.NoError(t, err)

		_, err = client.CreateUser(ctx, &appstreamsdk.CreateUserInput{
			UserName:           aws.String("slice18-usa-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)

		_, err = client.BatchAssociateUserStack(ctx, &appstreamsdk.BatchAssociateUserStackInput{
			UserStackAssociations: []types.UserStackAssociation{
				{
					UserName:           aws.String("slice18-usa-user@example.com"),
					StackName:          aws.String("slice18-usa-stack"),
					AuthenticationType: types.AuthenticationTypeUserpool,
				},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeUserStackAssociations(ctx, &appstreamsdk.DescribeUserStackAssociationsInput{
			StackName:          aws.String("slice18-usa-stack"),
			UserName:           aws.String("slice18-usa-user@example.com"),
			AuthenticationType: types.AuthenticationTypeUserpool,
		})
		require.NoError(t, err)
		require.Len(t, desc.UserStackAssociations, 1)

		disassocResult, err := client.BatchDisassociateUserStack(ctx, &appstreamsdk.BatchDisassociateUserStackInput{
			UserStackAssociations: []types.UserStackAssociation{
				{
					UserName:           aws.String("slice18-usa-user@example.com"),
					StackName:          aws.String("slice18-usa-stack"),
					AuthenticationType: types.AuthenticationTypeUserpool,
				},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, disassocResult.Errors)

		afterDisassoc, err := client.DescribeUserStackAssociations(
			ctx,
			&appstreamsdk.DescribeUserStackAssociationsInput{
				StackName:          aws.String("slice18-usa-stack"),
				UserName:           aws.String("slice18-usa-user@example.com"),
				AuthenticationType: types.AuthenticationTypeUserpool,
			},
		)
		require.NoError(t, err)
		assert.Empty(t, afterDisassoc.UserStackAssociations)
	})

	t.Run("sessions", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-sess-stack")})
		require.NoError(t, err)

		_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
			Name:         aws.String("slice18-sess-fleet"),
			InstanceType: aws.String("stream.standard.medium"),
			ImageName:    aws.String("some-image"),
			ComputeCapacity: &types.ComputeCapacity{
				DesiredInstances: aws.Int32(1),
			},
		})
		require.NoError(t, err)

		drainURL, err := client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
			StackName: aws.String("slice18-sess-stack"),
			FleetName: aws.String("slice18-sess-fleet"),
			UserId:    aws.String("slice18-drain-user"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(drainURL.StreamingURL))

		sessions, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
			StackName: aws.String("slice18-sess-stack"),
			FleetName: aws.String("slice18-sess-fleet"),
		})
		require.NoError(t, err)
		require.Len(t, sessions.Sessions, 1)
		drainSessionID := aws.ToString(sessions.Sessions[0].Id)

		_, err = client.DrainSessionInstance(ctx, &appstreamsdk.DrainSessionInstanceInput{
			SessionId: aws.String(drainSessionID),
		})
		require.NoError(t, err)

		expireURL, err := client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
			StackName: aws.String("slice18-sess-stack"),
			FleetName: aws.String("slice18-sess-fleet"),
			UserId:    aws.String("slice18-expire-user"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(expireURL.StreamingURL))

		sessionsAfterDrain, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
			StackName: aws.String("slice18-sess-stack"),
			FleetName: aws.String("slice18-sess-fleet"),
			UserId:    aws.String("slice18-expire-user"),
		})
		require.NoError(t, err)
		require.Len(t, sessionsAfterDrain.Sessions, 1)
		expireSessionID := aws.ToString(sessionsAfterDrain.Sessions[0].Id)

		_, err = client.ExpireSession(ctx, &appstreamsdk.ExpireSessionInput{
			SessionId: aws.String(expireSessionID),
		})
		require.NoError(t, err)
	})

	t.Run("usage_report_subscriptions", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateUsageReportSubscription(ctx, &appstreamsdk.CreateUsageReportSubscriptionInput{})
		require.NoError(t, err)

		_, err = client.DeleteUsageReportSubscription(ctx, &appstreamsdk.DeleteUsageReportSubscriptionInput{})
		require.NoError(t, err)
	})

	t.Run("themes", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18AppStreamClient(t)

		_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("slice18-theme-stack")})
		require.NoError(t, err)

		_, err = client.CreateThemeForStack(ctx, &appstreamsdk.CreateThemeForStackInput{
			StackName:    aws.String("slice18-theme-stack"),
			ThemeStyling: types.ThemeStylingBlue,
			TitleText:    aws.String("Slice18"),
			FaviconS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-theme"),
				S3Key:    aws.String("favicon.ico"),
			},
			OrganizationLogoS3Location: &types.S3Location{
				S3Bucket: aws.String("slice18-theme"),
				S3Key:    aws.String("logo.png"),
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeThemeForStack(ctx, &appstreamsdk.DescribeThemeForStackInput{
			StackName: aws.String("slice18-theme-stack"),
		})
		require.NoError(t, err)
		require.NotNil(t, desc.Theme)
		assert.Equal(t, "Slice18", aws.ToString(desc.Theme.ThemeTitleText))

		_, err = client.DeleteThemeForStack(ctx, &appstreamsdk.DeleteThemeForStackInput{
			StackName: aws.String("slice18-theme-stack"),
		})
		require.NoError(t, err)
	})
}
