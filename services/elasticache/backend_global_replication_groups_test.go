package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateGlobalReplicationGroup_FullFlow(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "global-primary",
		Description: "primary for global RG",
	})
	require.NoError(t, err)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "test-global", "test global", "global-primary")
	require.NoError(t, err)

	assert.Equal(t, "ldgnf-test-global", grg.GlobalReplicationGroupID)
	assert.Equal(t, "test global", grg.Description)
	assert.Equal(t, "available", grg.Status)
	assert.Equal(t, "us-east-1", grg.PrimaryReplicationGroupRegion)
	assert.NotNil(t, grg.SecondaryReplicationGroups)
	assert.NotEmpty(t, grg.ARN)
	assert.Contains(t, grg.ARN, "arn:aws:elasticache:")
}

func TestBackend_DescribeGlobalReplicationGroups_FilterByID(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	for _, id := range []string{"rg-primary-1", "rg-primary-2"} {
		_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
			ID: id, Description: "primary",
		})
		require.NoError(t, err)
	}

	_, err := b.CreateGlobalReplicationGroup(context.Background(), "g1", "global 1", "rg-primary-1")
	require.NoError(t, err)
	_, err = b.CreateGlobalReplicationGroup(context.Background(), "g2", "global 2", "rg-primary-2")
	require.NoError(t, err)

	p, err := b.DescribeGlobalReplicationGroups(context.Background(), "ldgnf-g1", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "ldgnf-g1", p.Data[0].GlobalReplicationGroupID)
}

func TestBackend_DeleteGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "del-grg-primary", Description: "for deletion",
	})
	require.NoError(t, err)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "to-del", "to delete", "del-grg-primary")
	require.NoError(t, err)
	assert.Equal(t, "ldgnf-to-del", grg.GlobalReplicationGroupID)

	deleted, err := b.DeleteGlobalReplicationGroup(context.Background(), "ldgnf-to-del", true)
	require.NoError(t, err)
	assert.Equal(t, "ldgnf-to-del", deleted.GlobalReplicationGroupID)
}

func TestBackend_ModifyGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "mod-grg-primary", Description: "for modification",
	})
	require.NoError(t, err)

	_, err = b.CreateGlobalReplicationGroup(context.Background(), "mod", "original desc", "mod-grg-primary")
	require.NoError(t, err)

	grg, err := b.ModifyGlobalReplicationGroup(context.Background(), "ldgnf-mod", "updated description", "", false)
	require.NoError(t, err)
	assert.Equal(t, "updated description", grg.Description)
}

// ----------------------------------------
// ServerlessCache CRUD (issue)
// ----------------------------------------

func TestBackend_IncreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "mygrg", "desc", "")
	require.NoError(t, err)
	initialCount := grg.NodeGroupCount

	updated, err := b.IncreaseNodeGroupsInGlobalReplicationGroup(context.Background(), grg.GlobalReplicationGroupID, 3)
	require.NoError(t, err)
	assert.Greater(t, updated.NodeGroupCount, initialCount)
	assert.Equal(t, int32(3), updated.NodeGroupCount)
}

func TestBackend_DecreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "dec-grg", "desc", "")
	require.NoError(t, err)

	_, err = b.IncreaseNodeGroupsInGlobalReplicationGroup(context.Background(), grg.GlobalReplicationGroupID, 5)
	require.NoError(t, err)

	updated, err := b.DecreaseNodeGroupsInGlobalReplicationGroup(context.Background(), grg.GlobalReplicationGroupID, 2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), updated.NodeGroupCount)
}

// ----------------------------------------
// DescribeEngineDefaultParameters — real defaults
// ----------------------------------------

func TestBackend_CreateGlobalReplicationGroup_PrimaryRegion(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "grg-primary",
		Description: "primary for global",
	})
	require.NoError(t, err)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "my-global", "global test", "grg-primary")
	require.NoError(t, err)

	assert.NotEmpty(t, grg.PrimaryReplicationGroupRegion)
	assert.Equal(t, "us-east-1", grg.PrimaryReplicationGroupRegion)
	assert.NotNil(t, grg.SecondaryReplicationGroups)
}
