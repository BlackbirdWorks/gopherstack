package kinesisanalyticsv2 //nolint:testpackage // needs access to the unexported region context key.

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

// TestKinesisAnalyticsV2ApplicationRegionIsolation proves that same-named applications
// created in two regions are fully isolated: each region sees only its own application,
// ARNs carry the correct region, and deleting in one region leaves the other intact.
func TestKinesisAnalyticsV2ApplicationRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create an application named "shared" in us-east-1.
	eastApp, err := backend.CreateApplication(
		ctxEast, "shared", "FLINK-1_18", "", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastApp.ApplicationARN, "us-east-1")

	// 2. Create an application with the SAME NAME in us-west-2; no conflict across regions.
	westApp, err := backend.CreateApplication(
		ctxWest, "shared", "FLINK-1_18", "", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westApp.ApplicationARN, "us-west-2")
	assert.NotEqual(t, eastApp.ApplicationARN, westApp.ApplicationARN)

	// 3. Each region lists only its own application.
	eastApps, _ := backend.ListApplications(ctxEast, "")
	require.Len(t, eastApps, 1)
	assert.Equal(t, "shared", eastApps[0].ApplicationName)
	assert.Contains(t, eastApps[0].ApplicationARN, "us-east-1")

	westApps, _ := backend.ListApplications(ctxWest, "")
	require.Len(t, westApps, 1)
	assert.Equal(t, "shared", westApps[0].ApplicationName)
	assert.Contains(t, westApps[0].ApplicationARN, "us-west-2")

	// 4. Describe-by-ARN resolves region from the ARN regardless of ctx region.
	got, err := backend.ListTagsForResource(ctxEast, westApp.ApplicationARN)
	require.NoError(t, err)
	assert.NotNil(t, got)

	// 5. Deleting in us-east-1 leaves us-west-2 intact.
	require.NoError(t, backend.DeleteApplication(ctxEast, "shared"))

	eastApps, _ = backend.ListApplications(ctxEast, "")
	assert.Empty(t, eastApps)

	westApps, _ = backend.ListApplications(ctxWest, "")
	assert.Len(t, westApps, 1)

	// The deleted east app is gone.
	_, err = backend.DescribeApplication(ctxEast, "shared")
	require.ErrorIs(t, err, ErrNotFound)

	// The west app is still describable.
	_, err = backend.DescribeApplication(ctxWest, "shared")
	require.NoError(t, err)
}

// TestKinesisAnalyticsV2SnapshotRegionIsolation proves that snapshots for
// same-named applications in different regions are fully isolated.
func TestKinesisAnalyticsV2SnapshotRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	_, err := backend.CreateApplication(ctxEast, "snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateApplication(ctxWest, "snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	err = backend.StartApplication(ctxEast, "snap-app")
	require.NoError(t, err)

	// Create snapshot on east app only.
	_, err = backend.CreateApplicationSnapshot(ctxEast, "snap-app", "snap-1")
	require.NoError(t, err)

	eastSnaps, _, err := backend.ListApplicationSnapshots(ctxEast, "snap-app", "")
	require.NoError(t, err)
	assert.Len(t, eastSnaps, 1)

	// West app has no snapshots.
	westSnaps, _, err := backend.ListApplicationSnapshots(ctxWest, "snap-app", "")
	require.NoError(t, err)
	assert.Empty(t, westSnaps)
}

// TestKinesisAnalyticsV2TagRegionIsolation proves that tags set on an application
// in one region do not appear on a same-named application in another region.
func TestKinesisAnalyticsV2TagRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastApp, err := backend.CreateApplication(
		ctxEast, "tagged-app", "FLINK-1_18", "", "", "",
		[]Tag{{Key: "region-tag", Value: "east"}},
	)
	require.NoError(t, err)

	westApp, err := backend.CreateApplication(
		ctxWest, "tagged-app", "FLINK-1_18", "", "", "", nil,
	)
	require.NoError(t, err)

	// East app has its tag; west app has none.
	eastTags, err := backend.ListTagsForResource(ctxEast, eastApp.ApplicationARN)
	require.NoError(t, err)
	assert.Len(t, eastTags, 1)
	assert.Equal(t, "east", eastTags[0].Value)

	westTags, err := backend.ListTagsForResource(ctxWest, westApp.ApplicationARN)
	require.NoError(t, err)
	assert.Empty(t, westTags)

	// Tagging east app does not affect west app.
	require.NoError(t, backend.TagResource(ctxEast, eastApp.ApplicationARN, []Tag{{Key: "team", Value: "data"}}))

	westTags, err = backend.ListTagsForResource(ctxWest, westApp.ApplicationARN)
	require.NoError(t, err)
	assert.Empty(t, westTags)
}
