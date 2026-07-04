package resourcegroups //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rgCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestResourceGroupsRegionIsolation proves that same-named groups created in two
// different regions are fully isolated: each region sees only its own groups,
// ARNs embed the correct region, and deleting in one region leaves the other untouched.
func TestResourceGroupsRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := rgCtxRegion("us-east-1")
	ctxWest := rgCtxRegion("us-west-2")

	// 1. Create a group with the SAME name in both regions.
	eastGroup, err := backend.CreateGroup(ctxEast, "shared-group", "east desc", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastGroup.ARN, "us-east-1")

	westGroup, err := backend.CreateGroup(ctxWest, "shared-group", "west desc", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westGroup.ARN, "us-west-2")

	// ARNs must differ (region-qualified) even though group names match.
	assert.NotEqual(t, eastGroup.ARN, westGroup.ARN)

	// 2. Each region reads back its own description.
	eastRead, err := backend.GetGroup(ctxEast, "shared-group")
	require.NoError(t, err)
	assert.Equal(t, "east desc", eastRead.Description)
	assert.Contains(t, eastRead.ARN, "us-east-1")

	westRead, err := backend.GetGroup(ctxWest, "shared-group")
	require.NoError(t, err)
	assert.Equal(t, "west desc", westRead.Description)
	assert.Contains(t, westRead.ARN, "us-west-2")

	// 3. ListGroups returns exactly one group per region.
	eastList, _ := backend.ListGroups(ctxEast, nil, "", 0)
	require.Len(t, eastList, 1)
	assert.Equal(t, "shared-group", eastList[0].Name)

	westList, _ := backend.ListGroups(ctxWest, nil, "", 0)
	require.Len(t, westList, 1)
	assert.Equal(t, "shared-group", westList[0].Name)

	// 4. Deleting in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteGroup(ctxEast, "shared-group"))

	eastGone, _ := backend.ListGroups(ctxEast, nil, "", 0)
	assert.Empty(t, eastGone)

	westStill, _ := backend.ListGroups(ctxWest, nil, "", 0)
	require.Len(t, westStill, 1)
	assert.Equal(t, "west desc", westStill[0].Description)
}

// TestResourceGroupsTagSyncTaskRegionIsolation proves that tag-sync tasks and
// group resources are isolated per region.
func TestResourceGroupsTagSyncTaskRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := rgCtxRegion("us-east-1")
	ctxWest := rgCtxRegion("us-west-2")

	// Create a group in each region.
	_, err := backend.CreateGroup(ctxEast, "app-group", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = backend.CreateGroup(ctxWest, "app-group", "", nil, nil, nil)
	require.NoError(t, err)

	// Group resources in us-east-1 only.
	_, err = backend.GroupResources(ctxEast, "app-group", []string{"arn:aws:s3:::east-bucket"})
	require.NoError(t, err)

	// us-east-1 sees the resource; us-west-2 does not.
	eastRes, _, err := backend.ListGroupResources(ctxEast, "app-group", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, eastRes, 1)
	assert.Equal(t, "arn:aws:s3:::east-bucket", eastRes[0].ResourceArn)

	westRes, _, err := backend.ListGroupResources(ctxWest, "app-group", nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, westRes)

	// SearchResources returns only the east resource from the east region.
	eastSearch, _, err := backend.SearchResources(ctxEast, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, eastSearch, 1)

	westSearch, _, err := backend.SearchResources(ctxWest, nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, westSearch)

	// Tag-sync task created in us-east-1 is not visible from us-west-2.
	task, err := backend.StartTagSyncTask(
		ctxEast, "app-group", "arn:aws:iam::000000000000:role/sync", "env", "prod", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, task.TaskArn, "us-east-1")

	eastTasks, _, err := backend.ListTagSyncTasks(ctxEast, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, eastTasks, 1)

	westTasks, _, err := backend.ListTagSyncTasks(ctxWest, nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, westTasks)

	// CancelTagSyncTask is scoped to the region: us-west-2 cannot cancel us-east-1 task.
	err = backend.CancelTagSyncTask(ctxWest, task.TaskArn)
	require.Error(t, err, "west region must not cancel east task ARN")
}

// TestResourceGroupsARNTagIsolation proves tag operations via ARN are region-scoped:
// an ARN created in one region is not resolvable from another.
func TestResourceGroupsARNTagIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := rgCtxRegion("us-east-1")
	ctxWest := rgCtxRegion("us-west-2")

	// Create group in us-east-1 and add tags.
	eastGroup, err := backend.CreateGroup(ctxEast, "tagged-group", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = backend.AddTagsByARN(ctxEast, eastGroup.ARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	// Tag lookup succeeds from us-east-1.
	eastTags, err := backend.GetTagsByARN(ctxEast, eastGroup.ARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", eastTags["env"])

	// The same ARN is not resolvable from us-west-2.
	_, err = backend.GetTagsByARN(ctxWest, eastGroup.ARN)
	require.Error(t, err, "east ARN must not be tag-resolvable from the west region")
}

// TestResourceGroupsDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestResourceGroupsDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	g, err := backend.CreateGroup(context.Background(), "def-group", "desc", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, g.ARN, "eu-central-1")

	// Reading via the explicit default region sees it.
	list, _ := backend.ListGroups(rgCtxRegion("eu-central-1"), nil, "", 0)
	require.Len(t, list, 1)
	assert.Equal(t, "def-group", list[0].Name)

	// A different region sees nothing.
	other, _ := backend.ListGroups(rgCtxRegion("ap-south-1"), nil, "", 0)
	assert.Empty(t, other)
}
