package redshift_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/redshift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *redshift.InMemoryBackend) string
		verify func(t *testing.T, b *redshift.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *redshift.InMemoryBackend) string {
				cluster, err := b.CreateCluster("test-cluster", "dc2.large", "testdb", "admin")
				if err != nil {
					return ""
				}

				return cluster.ClusterIdentifier
			},
			verify: func(t *testing.T, b *redshift.InMemoryBackend, id string) {
				t.Helper()

				clusters, err := b.DescribeClusters(id)
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].ClusterIdentifier)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *redshift.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *redshift.InMemoryBackend, _ string) {
				t.Helper()

				clusters, err := b.DescribeClusters("")
				require.NoError(t, err)
				assert.Empty(t, clusters)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
