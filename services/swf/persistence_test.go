package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
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
				err := b.RegisterDomain("test-domain", "test description", "30")
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
				assert.Equal(t, "30", domain.WorkflowExecutionRetentionPeriodInDays)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *swf.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *swf.InMemoryBackend, _ string) {
				t.Helper()

				domains, err := b.ListDomains("REGISTERED")
				require.NoError(t, err)
				assert.Empty(t, domains)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := swf.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := swf.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
