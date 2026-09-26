package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsvc55 "github.com/aws/aws-sdk-go-v2/service/datasync"
	datasynctypes55 "github.com/aws/aws-sdk-go-v2/service/datasync/types"
	fsxsvc55 "github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes55 "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_DatasyncFsxLocations provisions FSx Lustre/OpenZFS/ONTAP/Windows file
// systems and their DataSync FSx locations via Terraform, verifying each
// through the FSx and DataSync SDK Describe/List paths.
//
// aws_ec2_transit_gateway_connect_peer and
// aws_networkmanager_transit_gateway_connect_peer_association were dropped
// from this fixture: the create waiter never converged because our wire
// response never populated ConnectPeerConfiguration.BgpConfigurations,
// which the provider's own findTransitGatewayConnectPeer treats as
// not-found (gopherstack-zfrof). Fixed in b63f7384c; see
// ec2-transit-gateway-connect.tf for the now-covered resource pair.
func TestTerraform_DatasyncFsxLocations(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "datasync-fsx-locations",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyDatasyncFsxLocations(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func fsxTagName55(tags []fsxtypes55.Tag) string {
	for _, tag := range tags {
		if aws.ToString(tag.Key) == "Name" {
			return aws.ToString(tag.Value)
		}
	}

	return ""
}

func verifyDatasyncFsxLocations(ctx context.Context, t *testing.T) {
	t.Helper()

	fsxClient := createClientWithEndpoint(t, fsxsvc55.NewFromConfig, endpoint)

	fsOut, err := fsxClient.DescribeFileSystems(ctx, &fsxsvc55.DescribeFileSystemsInput{})
	require.NoError(t, err, "DescribeFileSystems should succeed")

	lustreFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "dsfx-lustre"
	}, "FSx Lustre file system dsfx-lustre")
	openzfsFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "dsfx-openzfs"
	}, "FSx OpenZFS file system dsfx-openzfs")
	ontapFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "dsfx-ontap"
	}, "FSx ONTAP file system dsfx-ontap")
	windowsFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "dsfx-windows"
	}, "FSx Windows file system dsfx-windows")

	assert.Equal(t, fsxtypes55.FileSystemTypeLustre, lustreFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeOpenzfs, openzfsFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeOntap, ontapFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeWindows, windowsFS.FileSystemType)

	svmOut, err := fsxClient.DescribeStorageVirtualMachines(ctx, &fsxsvc55.DescribeStorageVirtualMachinesInput{})
	require.NoError(t, err, "DescribeStorageVirtualMachines should succeed")

	svm := findBy(t, svmOut.StorageVirtualMachines, func(s fsxtypes55.StorageVirtualMachine) bool {
		return aws.ToString(s.Name) == "dsfxsvm"
	}, "storage virtual machine dsfxsvm")
	assert.Equal(t, aws.ToString(ontapFS.FileSystemId), aws.ToString(svm.FileSystemId))

	dsClient := createClientWithEndpoint(t, datasyncsvc55.NewFromConfig, endpoint)

	locOut, err := dsClient.ListLocations(ctx, &datasyncsvc55.ListLocationsInput{})
	require.NoError(t, err, "ListLocations should succeed")

	lustreLoc := findBy(t, locOut.Locations, func(l datasynctypes55.LocationListEntry) bool {
		return strings.Contains(aws.ToString(l.LocationUri), aws.ToString(lustreFS.FileSystemId))
	}, "DataSync FSx Lustre location for "+aws.ToString(lustreFS.FileSystemId))
	openzfsLoc := findBy(t, locOut.Locations, func(l datasynctypes55.LocationListEntry) bool {
		return strings.Contains(aws.ToString(l.LocationUri), aws.ToString(openzfsFS.FileSystemId))
	}, "DataSync FSx OpenZFS location for "+aws.ToString(openzfsFS.FileSystemId))
	ontapLoc := findBy(t, locOut.Locations, func(l datasynctypes55.LocationListEntry) bool {
		return strings.Contains(aws.ToString(l.LocationUri), aws.ToString(svm.StorageVirtualMachineId))
	}, "DataSync FSx ONTAP location for "+aws.ToString(svm.StorageVirtualMachineId))
	windowsLoc := findBy(t, locOut.Locations, func(l datasynctypes55.LocationListEntry) bool {
		return strings.Contains(aws.ToString(l.LocationUri), aws.ToString(windowsFS.FileSystemId))
	}, "DataSync FSx Windows location for "+aws.ToString(windowsFS.FileSystemId))

	lustreDesc, err := dsClient.DescribeLocationFsxLustre(ctx, &datasyncsvc55.DescribeLocationFsxLustreInput{
		LocationArn: lustreLoc.LocationArn,
	})
	require.NoError(t, err, "DescribeLocationFsxLustre should succeed")
	assert.True(t, strings.HasSuffix(aws.ToString(lustreDesc.LocationUri), "/dsfx"),
		"lustre location should keep the configured subdirectory")

	openzfsDesc, err := dsClient.DescribeLocationFsxOpenZfs(ctx, &datasyncsvc55.DescribeLocationFsxOpenZfsInput{
		LocationArn: openzfsLoc.LocationArn,
	})
	require.NoError(t, err, "DescribeLocationFsxOpenZfs should succeed")
	assert.NotEmpty(t, openzfsDesc.SecurityGroupArns)

	ontapDesc, err := dsClient.DescribeLocationFsxOntap(ctx, &datasyncsvc55.DescribeLocationFsxOntapInput{
		LocationArn: ontapLoc.LocationArn,
	})
	require.NoError(t, err, "DescribeLocationFsxOntap should succeed")
	assert.True(t, strings.HasSuffix(aws.ToString(ontapDesc.FsxFilesystemArn), aws.ToString(ontapFS.FileSystemId)),
		"ontap location's FsxFilesystemArn should reference the ontap file system")

	windowsDesc, err := dsClient.DescribeLocationFsxWindows(ctx, &datasyncsvc55.DescribeLocationFsxWindowsInput{
		LocationArn: windowsLoc.LocationArn,
	})
	require.NoError(t, err, "DescribeLocationFsxWindows should succeed")
	assert.Equal(t, "Admin", aws.ToString(windowsDesc.User))
}
