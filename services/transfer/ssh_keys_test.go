package transfer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestImportSSHPublicKey51stKeyReturnsError verifies the 50-key limit.
func TestImportSSHPublicKey51stKeyReturnsError(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	// Import 50 distinct keys.
	for i := range 50 {
		// Generate a synthetic-but-distinct key body (not real, but unique).
		// We bypass SSH parsing by using the internal store directly via a
		// fake key body that looks unique.  The backend doesn't validate key
		// authenticity; it only checks for duplicates and the count limit.
		fakeKey := fmt.Sprintf("ssh-ed25519 AAAA%06d test%d@example", i, i)
		_, err = b.ImportSSHPublicKey(s.ServerID, "alice", fakeKey)
		require.NoError(t, err, "key %d should import successfully", i)
	}

	assert.Equal(t, 50, transfer.SSHPublicKeyCount(b))

	// 51st key should fail.
	_, err = b.ImportSSHPublicKey(s.ServerID, "alice", "ssh-ed25519 AAAA999999 extra@example")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// TestImportSSHPublicKeyUnknownUserReturnsNotFound verifies that importing a key
// for a UserName that isn't a user on the server fails with ResourceNotFoundException,
// matching real AWS behavior (a user must exist before keys can be attached to it).
func TestImportSSHPublicKeyUnknownUserReturnsNotFound(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(s.ServerID, "nonexistent-user", "ssh-ed25519 AAAAABBBCCC nobody@example")
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrNotFound)
	assert.ErrorIs(t, err, transfer.ErrUserNotFound)
}
