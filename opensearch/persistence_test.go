package opensearch_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *opensearch.InMemoryBackend) string
		verify func(t *testing.T, b *opensearch.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *opensearch.InMemoryBackend) string {
				domain, err := b.CreateDomain("test-domain", "OpenSearch_2.3", opensearch.ClusterConfig{
					InstanceType:  "t3.small.search",
					InstanceCount: 1,
				})
				if err != nil {
					return ""
				}

				return domain.Name
			},
			verify: func(t *testing.T, b *opensearch.InMemoryBackend, id string) {
				t.Helper()

				domain, err := b.DescribeDomain(id)
				require.NoError(t, err)
				assert.Equal(t, id, domain.Name)
				assert.Equal(t, "OpenSearch_2.3", domain.EngineVersion)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *opensearch.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *opensearch.InMemoryBackend, _ string) {
				t.Helper()

				names := b.ListDomainNames()
				assert.Empty(t, names)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
