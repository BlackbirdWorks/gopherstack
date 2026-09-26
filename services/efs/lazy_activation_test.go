package efs //nolint:testpackage // needs access to unexported lifecycle-state constants.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCreateFileSystem_LazyActivation verifies that once fsActivationDelay is
// configured, a new file system reads as "creating" before the deadline and
// "available" after it, resolved lazily by effectiveFileSystemState instead
// of a background goroutine (see PARITY.md's leaks note).
func TestCreateFileSystem_LazyActivation(t *testing.T) {
	t.Parallel()

	const activationDelay = 50 * time.Millisecond

	tests := []struct {
		name      string
		wantState string
		wait      time.Duration
	}{
		{name: "immediately creating", wait: 0, wantState: statusCreating},
		{name: "after delay available", wait: activationDelay + time.Millisecond, wantState: statusAvailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				b := NewInMemoryBackend("123456789012", "us-east-1")
				b.fsActivationDelay = activationDelay

				fs, err := b.CreateFileSystem(ctx, CreateFileSystemRequest{CreationToken: "tok-1"})
				require.NoError(t, err)
				require.Equal(t, statusCreating, fs.LifeCycleState,
					"CreateFileSystem must return creating immediately, before any promotion")

				if tt.wait > 0 {
					time.Sleep(tt.wait)
				}

				got, _, err := b.DescribeFileSystems(ctx, fs.FileSystemID, "", "", 0)
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.Equal(t, tt.wantState, got[0].LifeCycleState)
			})
		})
	}
}

// TestCheckFileSystemAvailable_PromotesBeforeGating verifies that ops gated by
// checkFileSystemAvailable (e.g. CreateMountTarget's precondition) see the
// lazily-promoted "available" state once the activation deadline has passed,
// not just DescribeFileSystems callers.
func TestCheckFileSystemAvailable_PromotesBeforeGating(t *testing.T) {
	t.Parallel()

	const activationDelay = 50 * time.Millisecond

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		b := NewInMemoryBackend("123456789012", "us-east-1")
		b.fsActivationDelay = activationDelay

		fs, err := b.CreateFileSystem(ctx, CreateFileSystemRequest{CreationToken: "tok-2"})
		require.NoError(t, err)

		stored, ok := b.fileSystems.Get(regionKey("us-east-1", fs.FileSystemID))
		require.True(t, ok)

		require.ErrorIs(t, b.checkFileSystemAvailable(stored), ErrIncorrectFileSystemLifeCycleState)

		time.Sleep(activationDelay + time.Millisecond)

		require.NoError(t, b.checkFileSystemAvailable(stored))
	})
}
