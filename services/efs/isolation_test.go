package efs //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region, mirroring what the
// handler injects from the SigV4-derived request region.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestEFSRegionIsolation proves that same-named EFS resources created in two
// different regions are fully isolated: separate stores, separate ARNs, and
// operations in one region never observe or mutate the other.
func TestEFSRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a file system with the same creation token in both regions.
	eastFS, err := backend.CreateFileSystem(ctxEast, CreateFileSystemRequest{
		CreationToken:   "shared-token",
		Tags:            map[string]string{"Name": "shared"},
		ThroughputMode:  throughputModeBursting,
		PerformanceMode: performanceModeGeneral,
	})
	require.NoError(t, err)
	assert.Contains(t, eastFS.FileSystemArn, "us-east-1")
	assert.Equal(t, "us-east-1", eastFS.Region)

	westFS, err := backend.CreateFileSystem(ctxWest, CreateFileSystemRequest{
		CreationToken:   "shared-token",
		Tags:            map[string]string{"Name": "shared"},
		ThroughputMode:  throughputModeElastic,
		PerformanceMode: performanceModeGeneral,
	})
	require.NoError(t, err)
	assert.Contains(t, westFS.FileSystemArn, "us-west-2")
	assert.Equal(t, "us-west-2", westFS.Region)

	// The two file systems are distinct despite sharing a creation token.
	assert.NotEqual(t, eastFS.FileSystemID, westFS.FileSystemID)

	// 2. Each region sees only its own file system.
	eastList, _, err := backend.DescribeFileSystems(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, eastFS.FileSystemID, eastList[0].FileSystemID)
	assert.Equal(t, throughputModeBursting, eastList[0].ThroughputMode)

	westList, _, err := backend.DescribeFileSystems(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, westFS.FileSystemID, westList[0].FileSystemID)
	assert.Equal(t, throughputModeElastic, westList[0].ThroughputMode)

	// 3. Looking up the west file system ID in the east region must fail.
	_, _, err = backend.DescribeFileSystems(ctxEast, westFS.FileSystemID, "", "", 0)
	require.Error(t, err)

	// 4. Mount targets and access points are likewise isolated per region.
	eastMT, err := backend.CreateMountTarget(ctxEast, CreateMountTargetRequest{
		FileSystemID: eastFS.FileSystemID,
		SubnetID:     "subnet-east",
	})
	require.NoError(t, err)
	assert.Contains(t, eastMT.MountTargetArn, "us-east-1")

	westMT, err := backend.CreateMountTarget(ctxWest, CreateMountTargetRequest{
		FileSystemID: westFS.FileSystemID,
		SubnetID:     "subnet-west",
	})
	require.NoError(t, err)
	assert.Contains(t, westMT.MountTargetArn, "us-west-2")

	eastMTs, _, err := backend.DescribeMountTargets(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, eastMTs, 1)
	assert.Equal(t, eastMT.MountTargetID, eastMTs[0].MountTargetID)

	westMTs, _, err := backend.DescribeMountTargets(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, westMTs, 1)
	assert.Equal(t, westMT.MountTargetID, westMTs[0].MountTargetID)

	// 5. Tagging via the cross-index (ARN) store only affects the owning region.
	require.NoError(t, backend.TagResource(ctxEast, eastFS.FileSystemArn, map[string]string{"env": "east"}))

	// The east ARN is unknown in the west region.
	err = backend.TagResource(ctxWest, eastFS.FileSystemArn, map[string]string{"env": "west"})
	require.Error(t, err)

	eastTags, err := backend.ListTagsForResource(ctxEast, eastFS.FileSystemArn)
	require.NoError(t, err)
	assert.Equal(t, "east", eastTags["env"])

	// 6. Deleting the east file system (after removing its mount target) leaves
	//    the west region untouched.
	require.NoError(t, backend.DeleteMountTarget(ctxEast, eastMT.MountTargetID))
	require.NoError(t, backend.DeleteFileSystem(ctxEast, eastFS.FileSystemID))

	eastAfter, _, err := backend.DescribeFileSystems(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, eastAfter)

	westAfter, _, err := backend.DescribeFileSystems(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, westAfter, 1)
}

// TestEFSAccessPointRegionIsolation proves access points (and their client-token
// idempotency index) are isolated across regions.
func TestEFSAccessPointRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastFS, err := backend.CreateFileSystem(ctxEast, CreateFileSystemRequest{CreationToken: "ap-east"})
	require.NoError(t, err)

	westFS, err := backend.CreateFileSystem(ctxWest, CreateFileSystemRequest{CreationToken: "ap-west"})
	require.NoError(t, err)

	// Same client token in both regions yields independent access points.
	eastAP, err := backend.CreateAccessPoint(ctxEast, CreateAccessPointRequest{
		FileSystemID: eastFS.FileSystemID,
		ClientToken:  "tok",
	})
	require.NoError(t, err)
	assert.Contains(t, eastAP.AccessPointArn, "us-east-1")

	westAP, err := backend.CreateAccessPoint(ctxWest, CreateAccessPointRequest{
		FileSystemID: westFS.FileSystemID,
		ClientToken:  "tok",
	})
	require.NoError(t, err)
	assert.Contains(t, westAP.AccessPointArn, "us-west-2")
	assert.NotEqual(t, eastAP.AccessPointID, westAP.AccessPointID)

	eastAPs, _, err := backend.DescribeAccessPoints(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, eastAPs, 1)
	assert.Equal(t, eastAP.AccessPointID, eastAPs[0].AccessPointID)

	westAPs, _, err := backend.DescribeAccessPoints(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, westAPs, 1)
	assert.Equal(t, westAP.AccessPointID, westAPs[0].AccessPointID)

	// Deleting the east access point does not affect the west one.
	require.NoError(t, backend.DeleteAccessPoint(ctxEast, eastAP.AccessPointID))

	eastAfter, _, err := backend.DescribeAccessPoints(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, eastAfter)

	westAfter, _, err := backend.DescribeAccessPoints(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, westAfter, 1)
}
