package cloudwatchlogs //nolint:testpackage // existing issue.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsRegionIsolation(t *testing.T) { //nolint:paralleltest // existing issue.
	backend := NewInMemoryBackend()

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	// 1. Create log group in us-east-1
	_, err := backend.CreateLogGroup(ctxEast, "group1", "", "")
	require.NoError(t, err)

	// 2. Create log group with SAME NAME in us-west-2
	_, err = backend.CreateLogGroup(ctxWest, "group1", "", "")
	require.NoError(t, err)

	// 3. Verify us-east-1 only sees its group
	eastGroups, _, err := backend.DescribeLogGroups(ctxEast, "", "", 0)
	require.NoError(t, err)
	require.Len(t, eastGroups, 1)
	assert.Equal(t, "group1", eastGroups[0].LogGroupName)
	assert.Contains(t, eastGroups[0].Arn, "us-east-1")

	// 4. Verify us-west-2 only sees its group
	westGroups, _, err := backend.DescribeLogGroups(ctxWest, "", "", 0)
	require.NoError(t, err)
	require.Len(t, westGroups, 1)
	assert.Equal(t, "group1", westGroups[0].LogGroupName)
	assert.Contains(t, westGroups[0].Arn, "us-west-2")

	// 5. Delete in us-east-1 and verify still exists in us-west-2
	err = backend.DeleteLogGroup(ctxEast, "group1")
	require.NoError(t, err)

	eastGroups2, _, err := backend.DescribeLogGroups(ctxEast, "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, eastGroups2)

	westGroups2, _, err := backend.DescribeLogGroups(ctxWest, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, westGroups2, 1)
}

// TestCloudWatchLogsRegionIsolation_LookupTable proves CreateLookupTable's
// ARN (and hence its store key, lookupTableKeyFn) previously ignored the
// per-request region entirely -- ARNs and identity, unlike log groups
// above, were built from the backend's constant default region (b.region),
// never the ctx-derived one every other resource in this package uses. Two
// regions creating a same-named table therefore collided on one storage
// key: the second create failed with "already exists" even though it was a
// distinct regional resource in real AWS, and DescribeLookupTables leaked
// every region's tables to every caller regardless of which region asked.
func TestCloudWatchLogsRegionIsolation_LookupTable(t *testing.T) { //nolint:paralleltest // existing issue.
	backend := NewInMemoryBackend()

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	const csvBody = "col1,col2\nval1,val2\n"

	eastTable, err := backend.CreateLookupTable(ctxEast, "sharedname", csvBody, "", "", "")
	require.NoError(t, err, "creating a lookup table in us-east-1 must succeed")
	assert.Contains(t, eastTable.LookupTableArn, "us-east-1")

	westTable, err := backend.CreateLookupTable(ctxWest, "sharedname", csvBody, "", "", "")
	require.NoError(t, err, "creating a same-named lookup table in a DIFFERENT region must not collide")
	assert.Contains(t, westTable.LookupTableArn, "us-west-2")
	assert.NotEqual(t, eastTable.LookupTableArn, westTable.LookupTableArn,
		"the two regions' tables must not share a storage key")

	eastTables, _ := backend.DescribeLookupTables(ctxEast, "", "", 0)
	require.Len(t, eastTables, 1, "us-east-1 must only see its own lookup table")
	assert.Equal(t, "sharedname", eastTables[0].LookupTableName)
	assert.Contains(t, eastTables[0].LookupTableArn, "us-east-1")

	westTables, _ := backend.DescribeLookupTables(ctxWest, "", "", 0)
	require.Len(t, westTables, 1, "us-west-2 must only see its own lookup table")
	assert.Equal(t, "sharedname", westTables[0].LookupTableName)
	assert.Contains(t, westTables[0].LookupTableArn, "us-west-2")
}
