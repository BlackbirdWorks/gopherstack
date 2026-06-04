package appstream_test

import (
	"testing"

	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appstream client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	h := appstream.NewHandler(backend)

	// Operations not yet implemented — app blocks, image builders, entitlements,
	// users, directory configs, sessions, streaming URLs, usage reports, themes,
	// software associations, and org management.
	notImplemented := []string{
		"AssociateAppBlockBuilderAppBlock",
		"AssociateApplicationFleet",
		"AssociateApplicationToEntitlement",
		"AssociateSoftwareToImageBuilder",
		"BatchAssociateUserStack",
		"BatchDisassociateUserStack",
		"CopyImage",
		"CreateAppBlock",
		"CreateAppBlockBuilder",
		"CreateAppBlockBuilderStreamingURL",
		"CreateApplication",
		"CreateDirectoryConfig",
		"CreateEntitlement",
		"CreateExportImageTask",
		"CreateImageBuilder",
		"CreateImageBuilderStreamingURL",
		"CreateImportedImage",
		"CreateStreamingURL",
		"CreateThemeForStack",
		"CreateUpdatedImage",
		"CreateUsageReportSubscription",
		"CreateUser",
		"DeleteAppBlock",
		"DeleteAppBlockBuilder",
		"DeleteApplication",
		"DeleteDirectoryConfig",
		"DeleteEntitlement",
		"DeleteImage",
		"DeleteImageBuilder",
		"DeleteImagePermissions",
		"DeleteThemeForStack",
		"DeleteUsageReportSubscription",
		"DeleteUser",
		"DescribeAppBlockBuilderAppBlockAssociations",
		"DescribeAppBlockBuilders",
		"DescribeAppBlocks",
		"DescribeApplicationFleetAssociations",
		"DescribeApplications",
		"DescribeAppLicenseUsage",
		"DescribeDirectoryConfigs",
		"DescribeEntitlements",
		"DescribeImageBuilders",
		"DescribeImagePermissions",
		"DescribeImages",
		"DescribeSessions",
		"DescribeSoftwareAssociations",
		"DescribeThemeForStack",
		"DescribeUsageReportSubscriptions",
		"DescribeUsers",
		"DescribeUserStackAssociations",
		"DisableUser",
		"DisassociateAppBlockBuilderAppBlock",
		"DisassociateApplicationFleet",
		"DisassociateApplicationFromEntitlement",
		"DisassociateSoftwareFromImageBuilder",
		"DrainSessionInstance",
		"EnableUser",
		"ExpireSession",
		"GetExportImageTask",
		"ListEntitledApplications",
		"ListExportImageTasks",
		"StartAppBlockBuilder",
		"StartImageBuilder",
		"StartSoftwareDeploymentToImageBuilder",
		"StopAppBlockBuilder",
		"StopImageBuilder",
		"UpdateAppBlockBuilder",
		"UpdateApplication",
		"UpdateDirectoryConfig",
		"UpdateEntitlement",
		"UpdateImagePermissions",
		"UpdateThemeForStack",
	}

	sdkcheck.CheckCompleteness(t, &appstreamsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
