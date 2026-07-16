package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRemoveTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags         map[string]string
		wantAfterAdd    map[string]string
		wantAfterRemove map[string]string
		name            string
		removeTags      []string
	}{
		{
			name:            "add_and_remove_tags",
			addTags:         map[string]string{"env": "prod", "team": "platform"},
			removeTags:      []string{"team"},
			wantAfterAdd:    map[string]string{"env": "prod", "team": "platform"},
			wantAfterRemove: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)

			c, err := backend.CreateCluster(context.Background(), "tag-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			err = backend.AddTagsToResource(context.Background(), c.ARN, tt.addTags)
			require.NoError(t, err)

			got, err := backend.ListTagsForResource(context.Background(), c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAfterAdd, got)

			err = backend.RemoveTagsFromResource(context.Background(), c.ARN, tt.removeTags)
			require.NoError(t, err)

			got, err = backend.ListTagsForResource(context.Background(), c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAfterRemove, got)
		})
	}
}

func TestListTagsForResource_NilTagsSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_tags_returns_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)

			snap := backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			backend2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)
			require.NoError(t, backend2.Restore(t.Context(), snap))

			_, err := backend2.CreateCluster(context.Background(), "nil-tags-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			p, err := backend2.DescribeClusters(context.Background(), "nil-tags-cluster", "", 0, false)
			require.NoError(t, err)

			clusterARN := p.Data[0].ARN
			result, err := backend2.ListTagsForResource(context.Background(), clusterARN)
			require.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}

func TestBackend_Tags_ClusterCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	cl, err := b.CreateClusterWithOptions(
		context.Background(),
		"tag-test-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), cl.ARN, map[string]string{
		"env":  "test",
		"team": "platform",
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cl.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	err = b.RemoveTagsFromResource(context.Background(), cl.ARN, []string{"env"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), cl.ARN)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}

func TestBackend_Tags_ReplicationGroupCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	rg, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "tag-rg", Description: "tag test rg",
	})
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), rg.ARN, map[string]string{"stage": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), rg.ARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["stage"])
}

func TestBackend_Tags_UserCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	u, err := b.CreateUser(context.Background(), "tag-user", "tag-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), u.ARN, map[string]string{"owner": "alice"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), u.ARN)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])
}

func TestBackend_Tags_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"tag-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "tag-snap", "tag-snap-cl", "")
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), snap.ARN, map[string]string{"retention": "30days"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), snap.ARN)
	require.NoError(t, err)
	assert.Equal(t, "30days", tags["retention"])
}
