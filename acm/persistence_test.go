package acm_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/acm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *acm.InMemoryBackend) string
		verify func(t *testing.T, b *acm.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *acm.InMemoryBackend) string {
				cert, err := b.RequestCertificate("example.com", "AMAZON_ISSUED")
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				cert, err := b.DescribeCertificate(id)
				require.NoError(t, err)
				assert.Equal(t, "example.com", cert.DomainName)
				assert.Equal(t, id, cert.ARN)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *acm.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *acm.InMemoryBackend, _ string) {
				t.Helper()

				certs := b.ListCertificates()
				assert.Empty(t, certs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := acm.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := acm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
