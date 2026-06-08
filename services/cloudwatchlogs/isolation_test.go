package cloudwatchlogs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsRegionIsolation(t *testing.T) {
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
	assert.Len(t, eastGroups2, 0)

	westGroups2, _, err := backend.DescribeLogGroups(ctxWest, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, westGroups2, 1)
}
