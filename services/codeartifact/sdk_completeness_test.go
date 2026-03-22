package codeartifact_test

import (
	"testing"

	codeartifactsdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// codeartifact client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := codeartifact.NewInMemoryBackend("000000000000", "us-east-1")
	h := codeartifact.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &codeartifactsdk.Client{}, h.GetSupportedOperations(), []string{
		"AssociateExternalConnection",
		"CopyPackageVersions",
		"CreatePackageGroup",
		"DeletePackage",
		"DeletePackageGroup",
		"DeletePackageVersions",
		"DeleteRepositoryPermissionsPolicy",
		"DescribePackage",
		"DescribePackageGroup",
		"DescribePackageVersion",
		"DisassociateExternalConnection",
		"DisposePackageVersions",
		"GetAssociatedPackageGroup",
		"GetPackageVersionAsset",
		"GetPackageVersionReadme",
		"GetRepositoryPermissionsPolicy",
		"ListAllowedRepositoriesForGroup",
		"ListAssociatedPackages",
		"ListPackageGroups",
		"ListPackageVersionAssets",
		"ListPackageVersionDependencies",
		"ListPackageVersions",
		"ListPackages",
		"ListSubPackageGroups",
		"PublishPackageVersion",
		"PutPackageOriginConfiguration",
		"PutRepositoryPermissionsPolicy",
		"UpdatePackageGroup",
		"UpdatePackageGroupOriginConfiguration",
		"UpdatePackageVersionsStatus",
		"UpdateRepository",
	})
}
