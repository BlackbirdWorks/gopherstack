package kinesisanalytics //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kaCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestKinesisAnalyticsRegionIsolation verifies that same-named applications in
// two different regions are fully isolated: each region sees only its own
// resources, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestKinesisAnalyticsRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("us-east-1", "000000000000")

	ctxEast := kaCtxRegion("us-east-1")
	ctxWest := kaCtxRegion("us-west-2")

	// 1. Create same-named application in both regions.
	eastApp, err := b.CreateApplication(ctxEast, "shared-app", "east desc", "SELECT 1", nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastApp.ApplicationARN, "us-east-1")

	westApp, err := b.CreateApplication(ctxWest, "shared-app", "west desc", "SELECT 2", nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westApp.ApplicationARN, "us-west-2")

	// ARNs must differ even though names match.
	assert.NotEqual(t, eastApp.ApplicationARN, westApp.ApplicationARN)

	// 2. Each region reads back its own application.
	got, err := b.DescribeApplication(ctxEast, "shared-app")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", got.ApplicationCode)

	got, err = b.DescribeApplication(ctxWest, "shared-app")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2", got.ApplicationCode)

	// 3. ListApplications returns exactly one entry per region.
	eastList, _, err := b.ListApplications(ctxEast, "", 50)
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "SELECT 1", eastList[0].ApplicationCode)

	westList, _, err := b.ListApplications(ctxWest, "", 50)
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "SELECT 2", westList[0].ApplicationCode)

	// 4. Delete in west does not affect east.
	// DeleteApplication is async: it sets DELETING state first, then removes after
	// transitionDelay. We verify the west app enters DELETING state and the east
	// app is entirely unaffected.
	ts := time.Now()
	err = b.DeleteApplication(ctxWest, "shared-app", &ts)
	require.NoError(t, err)

	// West app transitions to DELETING (still visible, not yet removed).
	deletingApp, err := b.DescribeApplication(ctxWest, "shared-app")
	require.NoError(t, err)
	assert.Equal(t, statusDeleting, deletingApp.ApplicationStatus)

	// East app is completely unaffected.
	got, err = b.DescribeApplication(ctxEast, "shared-app")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", got.ApplicationCode)
	assert.Equal(t, statusReady, got.ApplicationStatus)

	// 5. Default region fallback: context.Background() → defaultRegion (us-east-1).
	got, err = b.DescribeApplication(context.Background(), "shared-app")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", got.ApplicationCode)
}
