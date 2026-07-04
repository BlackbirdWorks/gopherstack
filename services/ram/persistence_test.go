package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestRAM_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *ram.InMemoryBackend)
		verify func(t *testing.T, b *ram.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *ram.InMemoryBackend) {},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				assert.Empty(t, shares)
			},
		},
		{
			name: "resource_share_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateResourceShare(
					"my-share",
					true,
					map[string]string{"env": "test"},
					[]string{"arn:aws:iam::999:root"},
					[]string{"arn:aws:ec2:us-east-1:123:subnet/abc"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)
				assert.Equal(t, "my-share", shares[0].Name)
				assert.True(t, shares[0].AllowExternalPrincipals)
				assert.Equal(t, "test", shares[0].Tags["env"])

				// ARN-based lookup must work.
				got, err := b.GetResourceShare(shares[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "my-share", got.Name)
			},
		},
		{
			name: "associations_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				rs, err := b.CreateResourceShare("shared", true, nil, nil, nil)
				require.NoError(t, err)

				_, err = b.AssociateResourceShare(
					rs.ARN,
					[]string{"arn:aws:iam::777:root"},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)

				assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{shares[0].ARN})
				require.Len(t, assocs, 1)
				assert.Equal(t, "arn:aws:iam::777:root", assocs[0].AssociatedEntity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := ram.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
