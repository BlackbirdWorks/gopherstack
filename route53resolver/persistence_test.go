package route53resolver_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/route53resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *route53resolver.InMemoryBackend) string
		verify func(t *testing.T, b *route53resolver.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *route53resolver.InMemoryBackend) string {
				ep, err := b.CreateResolverEndpoint("test-ep", "INBOUND", "vpc-12345", []route53resolver.IPAddress{
					{SubnetID: "subnet-1", IP: "10.0.0.1"},
				})
				if err != nil {
					return ""
				}

				return ep.ID
			},
			verify: func(t *testing.T, b *route53resolver.InMemoryBackend, id string) {
				t.Helper()

				ep, err := b.GetResolverEndpoint(id)
				require.NoError(t, err)
				assert.Equal(t, "test-ep", ep.Name)
				assert.Equal(t, id, ep.ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *route53resolver.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *route53resolver.InMemoryBackend, _ string) {
				t.Helper()

				endpoints := b.ListResolverEndpoints()
				assert.Empty(t, endpoints)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
