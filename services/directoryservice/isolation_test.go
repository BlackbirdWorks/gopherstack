package directoryservice //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under regionContextKey,
// mirroring what the HTTP handler injects from the SigV4 credential scope.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestDirectoryRegionIsolation proves that directories created in two regions are
// fully isolated: each region sees only its own directories, deleting in one region
// leaves the other intact, and an ID created in one region is invisible from the other.
func TestDirectoryRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a directory in each region (same name is allowed across regions).
	eastDir, err := backend.CreateMicrosoftAD(
		ctxEast, "corp.example.com", "corp", "east dir", "", DirectoryEditionEnterprise, nil, nil,
	)
	require.NoError(t, err)

	westDir, err := backend.CreateMicrosoftAD(
		ctxWest, "corp.example.com", "corp", "west dir", "", DirectoryEditionStandard, nil, nil,
	)
	require.NoError(t, err)
	assert.NotEqual(t, eastDir.DirectoryID, westDir.DirectoryID)

	// 2. Each region lists only its own directory with its own attributes.
	eastList, _, err := backend.DescribeDirectories(ctxEast, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, eastDir.DirectoryID, eastList[0].DirectoryID)
	assert.Equal(t, "east dir", eastList[0].Description)

	westList, _, err := backend.DescribeDirectories(ctxWest, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, westDir.DirectoryID, westList[0].DirectoryID)
	assert.Equal(t, "west dir", westList[0].Description)

	// 3. The east directory ID is not resolvable from the west region.
	_, _, err = backend.DescribeDirectories(ctxWest, []string{eastDir.DirectoryID}, 0, "")
	require.ErrorIs(t, err, ErrDirectoryNotFound)

	// 4. Deleting in us-east-1 leaves us-west-2 intact.
	require.NoError(t, backend.DeleteDirectory(ctxEast, eastDir.DirectoryID))

	eastList, _, err = backend.DescribeDirectories(ctxEast, nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, eastList)

	westList, _, err = backend.DescribeDirectories(ctxWest, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, westList, 1)

	// 5. Deleting an east directory ID from the west region fails (not found there).
	require.ErrorIs(t, backend.DeleteDirectory(ctxWest, eastDir.DirectoryID), ErrDirectoryNotFound)
}

// TestDirectoryDefaultRegionFallback proves that a context without a region falls
// back to the backend's default region.
func TestDirectoryDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	// Created with no region in context -> backend default (us-east-1).
	dir, err := backend.CreateDirectory(
		context.Background(),
		"fallback.example.com",
		"fb",
		"",
		"",
		DirectorySizeSmall,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Visible from the explicit default region.
	list, _, err := backend.DescribeDirectories(ctxRegion("us-east-1"), nil, 0, "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, dir.DirectoryID, list[0].DirectoryID)

	// Not visible from a different region.
	other, _, err := backend.DescribeDirectories(ctxRegion("eu-west-1"), nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, other)
}

// TestDependentResourceRegionIsolation proves that resources that hang off a
// directory (snapshots, trusts, certificates, conditional forwarders, IP routes,
// tags) are isolated per region along with their parent directory.
func TestDependentResourceRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastDir, err := backend.CreateMicrosoftAD(
		ctxEast,
		"corp.example.com",
		"corp",
		"",
		"",
		DirectoryEditionEnterprise,
		nil,
		nil,
	)
	require.NoError(t, err)

	westDir, err := backend.CreateMicrosoftAD(
		ctxWest,
		"corp.example.com",
		"corp",
		"",
		"",
		DirectoryEditionEnterprise,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Snapshot isolation: a snapshot of the east directory is invisible from west.
	eastSnap, err := backend.CreateSnapshot(ctxEast, eastDir.DirectoryID, "east-snap")
	require.NoError(t, err)

	eastSnaps, _, err := backend.DescribeSnapshots(ctxEast, eastDir.DirectoryID, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, eastSnaps, 1)
	assert.Equal(t, eastSnap.SnapshotID, eastSnaps[0].SnapshotID)

	westSnaps, _, err := backend.DescribeSnapshots(ctxWest, "", nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, westSnaps)

	// The east snapshot cannot be deleted from the west region.
	require.ErrorIs(t, backend.DeleteSnapshot(ctxWest, eastSnap.SnapshotID), ErrSnapshotNotFound)

	// Trust isolation.
	eastTrust, err := backend.CreateTrust(
		ctxEast,
		eastDir.DirectoryID,
		"remote.example.com",
		"pw",
		"Two-Way",
		"Forest",
	)
	require.NoError(t, err)

	_, err = backend.UpdateTrust(ctxWest, eastTrust, "Enabled")
	require.ErrorIs(t, err, ErrTrustNotFound)

	eastTrusts, _, err := backend.DescribeTrusts(ctxEast, eastDir.DirectoryID, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, eastTrusts, 1)

	westTrusts, _, err := backend.DescribeTrusts(ctxWest, westDir.DirectoryID, nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, westTrusts)

	// Certificate isolation.
	eastCert, err := backend.RegisterCertificate(
		ctxEast,
		eastDir.DirectoryID,
		"cert-data",
		"ClientLDAPS",
	)
	require.NoError(t, err)

	_, err = backend.DescribeCertificate(ctxWest, westDir.DirectoryID, eastCert)
	require.ErrorIs(t, err, ErrCertNotFound)

	cert, err := backend.DescribeCertificate(ctxEast, eastDir.DirectoryID, eastCert)
	require.NoError(t, err)
	assert.Equal(t, eastCert, cert.CertificateID)

	// Conditional forwarder isolation.
	require.NoError(
		t,
		backend.CreateConditionalForwarder(
			ctxEast,
			eastDir.DirectoryID,
			"fwd.example.com",
			[]string{"10.0.0.1"},
		),
	)

	westFwds, err := backend.DescribeConditionalForwarders(ctxWest, westDir.DirectoryID, nil)
	require.NoError(t, err)
	assert.Empty(t, westFwds)

	eastFwds, err := backend.DescribeConditionalForwarders(ctxEast, eastDir.DirectoryID, nil)
	require.NoError(t, err)
	assert.Len(t, eastFwds, 1)

	// IP route isolation.
	require.NoError(
		t,
		backend.AddIpRoutes(ctxEast, eastDir.DirectoryID, []IpRoute{{CidrIP: "10.0.0.0/16"}}),
	)

	eastRoutes, _, err := backend.ListIpRoutes(ctxEast, eastDir.DirectoryID, 0, "")
	require.NoError(t, err)
	assert.Len(t, eastRoutes, 1)

	// Tag isolation: tagging the east directory does not leak to the same-named west directory.
	require.NoError(
		t,
		backend.AddTagsToResource(ctxEast, eastDir.DirectoryID, []Tag{{Key: "env", Value: "east"}}),
	)

	westTags, _, err := backend.ListTagsForResource(ctxWest, westDir.DirectoryID, 0, "")
	require.NoError(t, err)
	assert.Empty(t, westTags)

	eastTags, _, err := backend.ListTagsForResource(ctxEast, eastDir.DirectoryID, 0, "")
	require.NoError(t, err)
	require.Len(t, eastTags, 1)
	assert.Equal(t, "east", eastTags[0].Value)
}

// TestADAssessmentRecordsContextRegion proves region-derived metadata reflects the
// request-context region rather than the backend default.
func TestADAssessmentRecordsContextRegion(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxWest := ctxRegion("us-west-2")

	dir, err := backend.CreateMicrosoftAD(
		ctxWest,
		"corp.example.com",
		"corp",
		"",
		"",
		DirectoryEditionEnterprise,
		nil,
		nil,
	)
	require.NoError(t, err)

	assessID, err := backend.StartADAssessment(ctxWest, dir.DirectoryID)
	require.NoError(t, err)

	info, err := backend.DescribeADAssessment(ctxWest, dir.DirectoryID, assessID)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", info.Region)
}
