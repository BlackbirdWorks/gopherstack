package elasticache_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all_state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)

			_, err := backend.CreateCluster(context.Background(), "reset-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			_, err = backend.CreateReplicationGroup(context.Background(), "reset-rg", "test")
			require.NoError(t, err)

			backend.Reset()

			_, err = backend.DescribeClusters(context.Background(), "reset-cluster", "", 0, false)
			require.ErrorIs(t, err, elasticache.ErrClusterNotFound)

			_, err = backend.DescribeReplicationGroups(context.Background(), "reset-rg", "", 0)
			require.ErrorIs(t, err, elasticache.ErrReplicationGroupNotFound)

			p, err := backend.DescribeEvents(context.Background(), "", "", "", time.Time{}, time.Time{}, 0, 0)
			require.NoError(t, err)
			assert.Empty(t, p.Data)

			_ = tt.name
		})
	}
}

func TestBackend_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(context.Background(), "reset-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "reset-rg", Description: "for reset",
	})
	require.NoError(t, err)

	_, err = b.CreateUser(context.Background(), "reset-user", "reset-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	b.Reset()

	// All resources should be gone.
	p1, err := b.DescribeClusters(context.Background(), "", "", 0, false)
	require.NoError(t, err)
	assert.Empty(t, p1.Data)

	p2, err := b.DescribeReplicationGroups(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p2.Data)

	p3, err := b.DescribeUsers(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p3.Data)
}
