package ram_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-west-2")
	assert.Equal(t, "us-west-2", b.Region())
}

func TestReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createShares   int
		wantAfterReset int
	}{
		{
			name:           "reset clears all shares",
			createShares:   3,
			wantAfterReset: 0,
		},
		{
			name:           "reset on empty backend is a no-op",
			createShares:   0,
			wantAfterReset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.createShares {
				_, err := b.CreateResourceShare(
					fmt.Sprintf("share-%d", i),
					true, nil, nil, nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			shares := b.ListResourceShares("SELF", "")
			assert.Len(t, shares, tt.wantAfterReset)
		})
	}
}

// TestRefinement1_Reset_ClearsAll verifies that Reset removes user state and re-seeds built-ins.
func TestReset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare("reset-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePermission("reset-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, ram.ResourceShareCount(b))
	// Built-in permissions are re-seeded after reset.
	assert.Equal(t, ram.BuiltInPermissionCount, ram.PermissionCount(b))
	assert.Equal(t, 0, ram.InvitationCount(b))
	assert.Equal(t, 0, ram.AssociationCount(b))
}
