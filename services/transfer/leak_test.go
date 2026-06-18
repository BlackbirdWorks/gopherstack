package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestImportSSHPublicKey_BodyIndexBounded verifies that importing and then deleting
// SSH public keys keeps the body uniqueness index in sync with the key store, so
// neither grows without bound across repeated import/delete cycles.
func TestImportSSHPublicKey_BodyIndexBounded(t *testing.T) {
	t.Parallel()

	// ed25519 test keys generated with ssh-keygen (safe for test use, not real).
	testKeys := []string{
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBqzG1vdEIp+OOmEzJMFEXOvqOAvj0c9IITqKMqL5ooH test-key-0",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHuZ3FwQFjbQcXbRaJ6WUQX9dLbY6sO7lkPiKBrvBqoR test-key-1",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOyBJU9bfZCE3h4kTyEIFiHd0NxzM7pzZc9lnABf+3eK test-key-2",
	}

	tests := []struct {
		name string
		keys []string
	}{
		{name: "single key", keys: testKeys[:1]},
		{name: "multiple keys", keys: testKeys},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend("123456789012", "us-east-1")

			srv, err := b.CreateServer([]string{"SFTP"}, nil)
			require.NoError(t, err)

			_, err = b.CreateUser(srv.ServerID, "alice", "", "", nil)
			require.NoError(t, err)

			keyIDs := make([]string, 0, len(tc.keys))

			for _, body := range tc.keys {
				k, importErr := b.ImportSSHPublicKey(srv.ServerID, "alice", body)
				require.NoError(t, importErr)
				keyIDs = append(keyIDs, k.SSHPublicKeyID)
			}

			require.Equal(t, len(tc.keys), transfer.SSHPublicKeyCount(b), "key count before delete")
			require.Equal(t, len(tc.keys), transfer.SSHKeyBodyIndexCount(b), "body index before delete")

			for _, id := range keyIDs {
				deleteErr := b.DeleteSSHPublicKey(srv.ServerID, "alice", id)
				require.NoError(t, deleteErr)
			}

			require.Equal(t, 0, transfer.SSHPublicKeyCount(b), "key store must be empty after delete")
			require.Equal(t, 0, transfer.SSHKeyBodyIndexCount(b), "body index must be empty after delete — no leak")
		})
	}
}

// TestImportSSHPublicKey_DuplicateRejectedByIndex verifies that importing the same key
// body twice is rejected via the O(1) index lookup, not an O(n) scan.
func TestImportSSHPublicKey_DuplicateRejectedByIndex(t *testing.T) {
	t.Parallel()

	const keyBody = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBqzG1vdEIp+OOmEzJMFEXOvqOAvj0c9IITqKMqL5ooH test-dup"

	b := transfer.NewInMemoryBackend("123456789012", "us-east-1")

	srv, err := b.CreateServer([]string{"SFTP"}, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(srv.ServerID, "alice", "", "", nil)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(srv.ServerID, "alice", keyBody)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(srv.ServerID, "alice", keyBody)
	require.Error(t, err, "duplicate key body must be rejected")

	require.Equal(t, 1, transfer.SSHPublicKeyCount(b), "only one key must be stored")
	require.Equal(t, 1, transfer.SSHKeyBodyIndexCount(b), "body index must have exactly one entry")
}
