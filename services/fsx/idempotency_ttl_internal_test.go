package fsx

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateFileSystemTokens_TTLBoundsMapGrowth locks in the fix for
// b.createFileSystemTokens growing unbounded: entries were never deleted, even
// after the file system behind them was deleted, so a long-running backend fed
// unique ClientRequestTokens leaked memory forever. CreateFileSystem now sweeps
// entries past createFileSystemTokenTTL on every write, and
// dedupCreateFileSystemLocked now honors that TTL on lookup.
func TestCreateFileSystemTokens_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	newInput := func(token string) *createFileSystemInput {
		return &createFileSystemInput{
			FileSystemType:      fileSystemTypeLustre,
			LustreConfiguration: &createLustreConfiguration{DeploymentType: lustreDeploymentTypeScratch1},
			ClientRequestToken:  token,
		}
	}

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")

		first, err := b.CreateFileSystem(newInput("tok-1"))
		require.NoError(t, err)
		assert.Len(t, b.createFileSystemTokens, 1, "one entry recorded after the first CreateFileSystem call")

		// Still inside the window: same token + same input replays the original file system.
		time.Sleep(createFileSystemTokenTTL - time.Second)

		replay, err := b.CreateFileSystem(newInput("tok-1"))
		require.NoError(t, err)
		assert.Equal(t, first.FileSystemID, replay.FileSystemID,
			"a replay inside the window must return the original resource, not create a new one")
		assert.Len(t, b.createFileSystemTokens, 1, "replay must not grow the map")

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		second, err := b.CreateFileSystem(newInput("tok-2"))
		require.NoError(t, err)
		assert.NotEqual(t, first.FileSystemID, second.FileSystemID)
		assert.Len(t, b.createFileSystemTokens, 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.createFileSystemTokens["tok-1"]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")

		// A retry of the expired token now creates a genuinely new resource
		// instead of replaying the (no longer remembered) original.
		third, err := b.CreateFileSystem(newInput("tok-1"))
		require.NoError(t, err)
		assert.NotEqual(t, first.FileSystemID, third.FileSystemID,
			"an expired token must not replay the original resource")
	})
}
