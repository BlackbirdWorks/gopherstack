package swf_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/swf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *swf.InMemoryBackend) string
		verify func(t *testing.T, b *swf.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *swf.InMemoryBackend) string {
				err := b.RegisterDomain("test-domain", "test description")
				if err != nil {
					return ""
				}

				return "test-domain"
			},
			verify: func(t *testing.T, b *swf.InMemoryBackend, id string) {
				t.Helper()

				domain, err := b.DescribeDomain(id)
				require.NoError(t, err)
				assert.Equal(t, id, domain.Name)
				assert.Equal(t, "test description", domain.Description)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *swf.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *swf.InMemoryBackend, _ string) {
				t.Helper()

				domains := b.ListDomains("REGISTERED")
				assert.Empty(t, domains)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := swf.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := swf.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
