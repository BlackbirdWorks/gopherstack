package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestDeleteReplication_ProtectionFlip verifies source protection resets to DISABLED.
func TestDeleteReplication_ProtectionFlip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantProtection string
	}{
		{
			name:           "protection_resets_to_disabled_after_delete",
			wantProtection: "DISABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			fs, err := b.CreateFileSystem(context.Background(), fsReq("tok-repl-prot-"+tt.name))
			require.NoError(t, err)

			_, err = b.CreateReplicationConfiguration(
				context.Background(),
				fs.FileSystemID,
				[]efs.ReplicationDestination{
					{Region: "us-west-2", Status: "ENABLED"},
				},
			)
			require.NoError(t, err)

			// After create, source should be REPLICATING.
			list, _, err := b.DescribeFileSystems(context.Background(), fs.FileSystemID, "", "", 0)
			require.NoError(t, err)
			require.Len(t, list, 1)
			assert.Equal(t, "REPLICATING", list[0].ReplicationOverwriteProtection)

			// Delete the replication config.
			err = b.DeleteReplicationConfiguration(context.Background(), fs.FileSystemID)
			require.NoError(t, err)

			// Source protection should revert.
			list2, _, err := b.DescribeFileSystems(context.Background(), fs.FileSystemID, "", "", 0)
			require.NoError(t, err)
			require.Len(t, list2, 1)
			assert.Equal(t, tt.wantProtection, list2[0].ReplicationOverwriteProtection)
		})
	}
}
