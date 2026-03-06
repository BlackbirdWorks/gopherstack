package elasticache_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *elasticache.InMemoryBackend) string
		verify func(t *testing.T, b *elasticache.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *elasticache.InMemoryBackend) string {
				cluster, err := b.CreateCluster("test-cluster", "redis", "cache.t3.micro", 6379)
				if err != nil {
					return ""
				}

				return cluster.ClusterID
			},
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, id string) {
				t.Helper()

				p, err := b.DescribeClusters(id, "", 0)
				clusters := p.Data
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].ClusterID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *elasticache.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, _ string) {
				t.Helper()

				clusters := b.ListAll()
				assert.Empty(t, clusters)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
