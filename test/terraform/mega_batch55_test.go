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

// TestTerraform_MegaBatch55 provisions FSx Lustre/OpenZFS/ONTAP/Windows file
// systems and their DataSync FSx locations via Terraform, verifying each
// through the FSx and DataSync SDK Describe/List paths.
//
// aws_ec2_transit_gateway_connect_peer and
// aws_networkmanager_transit_gateway_connect_peer_association were dropped
// from this batch: the pinned hashicorp/aws v5.100.0 provider's create
// waiter for the connect peer never converges, even though
// DescribeTransitGatewayConnectPeers returns the peer with State=available
// on every poll. Confirmed via TF_LOG=debug against a bare local server: the
// provider repeats identical successful describes (single item, correct
// TransitGatewayAttachmentId, State "available") for 21 retries before
// failing with "couldn't find resource". The wire shape matches the pinned
// aws-sdk-go-v2 ec2 deserializer exactly (transitGatewayConnectPeerSet/item
// wrapper, state/transitGatewayAttachmentId/transitGatewayConnectPeerId
// element names), so this is not a wire-shape mismatch fixable by
// inspection; see gopherstack-zfrof, left open.
func TestTerraform_MegaBatch55(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-55",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch55(ctx, t)
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

func verifyMegaBatch55(ctx context.Context, t *testing.T) {
	t.Helper()

	fsxClient := createClientWithEndpoint(t, fsxsvc55.NewFromConfig, endpoint)

	fsOut, err := fsxClient.DescribeFileSystems(ctx, &fsxsvc55.DescribeFileSystemsInput{})
	require.NoError(t, err, "DescribeFileSystems should succeed")

	lustreFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "mega-batch-55-lustre"
	}, "FSx Lustre file system mega-batch-55-lustre")
	openzfsFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "mega-batch-55-openzfs"
	}, "FSx OpenZFS file system mega-batch-55-openzfs")
	ontapFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "mega-batch-55-ontap"
	}, "FSx ONTAP file system mega-batch-55-ontap")
	windowsFS := findBy(t, fsOut.FileSystems, func(fs fsxtypes55.FileSystem) bool {
		return fsxTagName55(fs.Tags) == "mega-batch-55-windows"
	}, "FSx Windows file system mega-batch-55-windows")

	assert.Equal(t, fsxtypes55.FileSystemTypeLustre, lustreFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeOpenzfs, openzfsFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeOntap, ontapFS.FileSystemType)
	assert.Equal(t, fsxtypes55.FileSystemTypeWindows, windowsFS.FileSystemType)

	svmOut, err := fsxClient.DescribeStorageVirtualMachines(ctx, &fsxsvc55.DescribeStorageVirtualMachinesInput{})
	require.NoError(t, err, "DescribeStorageVirtualMachines should succeed")

	svm := findBy(t, svmOut.StorageVirtualMachines, func(s fsxtypes55.StorageVirtualMachine) bool {
		return aws.ToString(s.Name) == "mb55svm"
	}, "storage virtual machine mb55svm")
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
	assert.True(t, strings.HasSuffix(aws.ToString(lustreDesc.LocationUri), "/mb55"),
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
