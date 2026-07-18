package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_DescribeServiceUpdates_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeServiceUpdates(context.Background(), "", "", 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeUpdateActions_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeUpdateActions(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_BatchApplyUpdateAction_Processed(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "batch-rg-1", Description: "batch apply test",
	})
	require.NoError(t, err)

	result, err := b.BatchApplyUpdateAction(context.Background(),
		[]string{"batch-rg-1"},
		[]string{"missing-cluster"},
		"update-20260101",
	)
	require.NoError(t, err)
	assert.Len(t, result.ProcessedUpdateActions, 1)
	assert.Len(t, result.UnprocessedUpdateActions, 1)
}

func TestBackend_BatchStopUpdateAction(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"batch-stop-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	result, err := b.BatchStopUpdateAction(context.Background(),
		[]string{"missing-rg"},
		[]string{"batch-stop-cl"},
		"update-20260101",
	)
	require.NoError(t, err)
	assert.Len(t, result.ProcessedUpdateActions, 1)
	assert.Len(t, result.UnprocessedUpdateActions, 1)
}

// ----------------------------------------
// Tags — CRUD on multiple resource types
// ----------------------------------------

func TestBackend_BatchApplyUpdateAction_TracksUpdateActions(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "track-rg", Description: "tracking",
	})
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(context.Background(),
		[]string{"track-rg"},
		nil,
		"20240101-001-security-patch",
	)
	require.NoError(t, err)

	actions := b.ListUpdateActionsByServiceUpdate("20240101-001-security-patch")
	require.Len(t, actions, 1)
	assert.Equal(t, "track-rg", actions[0].ReplicationGroupID)
	assert.Equal(t, "20240101-001-security-patch", actions[0].ServiceUpdateName)
}

func TestBackend_BatchApplyUpdateAction_MultipleTargets(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	for _, id := range []string{"multi-rg-1", "multi-rg-2"} {
		_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
			ID: id, Description: "multi",
		})
		require.NoError(t, err)
	}

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"multi-cl-1",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(context.Background(),
		[]string{"multi-rg-1", "multi-rg-2"},
		[]string{"multi-cl-1"},
		"multi-patch",
	)
	require.NoError(t, err)

	actions := b.ListUpdateActionsByServiceUpdate("multi-patch")
	assert.Len(t, actions, 3)
}

// ----------------------------------------
// DescribeServiceUpdatesFull — seeded updates
// ----------------------------------------

func TestBackend_DescribeServiceUpdatesFull_SeededData(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	data, _, err := b.DescribeServiceUpdatesFull("", nil, "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(data), 1)
}

func TestBackend_DescribeServiceUpdatesFull_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	data, _, err := b.DescribeServiceUpdatesFull("20240101-001-security-patch", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, data, 1)
	assert.Equal(t, "20240101-001-security-patch", data[0].ServiceUpdateName)
}

func TestBackend_DescribeServiceUpdatesFull_FilterByStatus(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	data, _, err := b.DescribeServiceUpdatesFull("", []string{"available"}, "", 0)
	require.NoError(t, err)
	for _, su := range data {
		assert.Equal(t, "available", su.Status)
	}
}

// ----------------------------------------
// DescribeUpdateActionsFull
// ----------------------------------------

func TestBackend_DescribeUpdateActionsFull_FilterByUpdateName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "ua-filter-rg", Description: "filter",
	})
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(context.Background(), []string{"ua-filter-rg"}, nil, "patch-a")
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(context.Background(), []string{"ua-filter-rg"}, nil, "patch-b")
	require.NoError(t, err)

	data, _, err := b.DescribeUpdateActionsFull("patch-a", "", 0)
	require.NoError(t, err)
	require.Len(t, data, 1)
	assert.Equal(t, "patch-a", data[0].ServiceUpdateName)
}

// ----------------------------------------
// GlobalReplicationGroup NodeGroupCount
// ----------------------------------------

func TestBackend_AppendUpdateActions(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	actions := []*elasticache.UpdateAction{
		{ReplicationGroupID: "rg-1", ServiceUpdateName: "upd-1", UpdateActionStatus: "scheduling"},
		{ReplicationGroupID: "rg-2", ServiceUpdateName: "upd-1", UpdateActionStatus: "scheduling"},
	}

	b.AppendUpdateActions(actions)

	got := b.ListUpdateActionsByServiceUpdate("upd-1")
	assert.Len(t, got, 2)

	none := b.ListUpdateActionsByServiceUpdate("nonexistent")
	assert.Empty(t, none)
}
