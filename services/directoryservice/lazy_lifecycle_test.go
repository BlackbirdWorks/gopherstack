package directoryservice //nolint:testpackage // needs access to unexported lifecycle-delay constants.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDirectoryCreation_LazyLifecycleTransition verifies CreateDirectory
// starts a directory at Requested and, without any goroutine outliving the
// call, transitions it through Creating to Active on the worker.Group timers
// scheduled by transitionDirectoryToActive (see PARITY.md's leaks note).
func TestDirectoryCreation_LazyLifecycleTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantStage DirectoryStage
		wait      time.Duration
	}{
		{name: "immediately requested", wait: 0, wantStage: DirectoryStageRequested},
		{
			name:      "after one delay creating",
			wait:      directoryLifecycleDelay + time.Millisecond,
			wantStage: DirectoryStageCreating,
		},
		{
			name:      "after two delays active",
			wait:      2*directoryLifecycleDelay + time.Millisecond,
			wantStage: DirectoryStageActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")
				defer b.Close()

				created, err := b.CreateDirectory(
					context.Background(), "corp.example.com", "CORP", "", "Admin1234!",
					DirectorySizeSmall, "", nil, nil,
				)
				require.NoError(t, err)
				require.Equal(t, DirectoryStageRequested, created.Stage,
					"CreateDirectory must return Requested immediately, before any transition")

				if tt.wait > 0 {
					time.Sleep(tt.wait)
				}

				got, _, err := b.DescribeDirectories(context.Background(), []string{created.DirectoryID}, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.Equal(t, tt.wantStage, got[0].Stage)
			})
		})
	}
}

// TestRestoreFromSnapshot_LazyLifecycleTransition verifies RestoreFromSnapshot
// holds Restoring until restoreLifecycleDelay elapses and then settles at
// Active, driven by the same worker.Group as directory creation.
func TestRestoreFromSnapshot_LazyLifecycleTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantStage DirectoryStage
		wait      time.Duration
	}{
		{name: "immediately restoring", wait: 0, wantStage: DirectoryStageRestoring},
		{
			name:      "after delay active",
			wait:      restoreLifecycleDelay + time.Millisecond,
			wantStage: DirectoryStageActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				b := NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")
				defer b.Close()

				created, err := b.CreateDirectory(
					ctx, "corp.example.com", "CORP", "", "Admin1234!", DirectorySizeSmall, "", nil, nil,
				)
				require.NoError(t, err)

				// Let creation finish so the restore transition is the only one in flight.
				time.Sleep(2*directoryLifecycleDelay + time.Millisecond)

				snap, err := b.CreateSnapshot(ctx, created.DirectoryID, "snap-1")
				require.NoError(t, err)

				require.NoError(t, b.RestoreFromSnapshot(ctx, snap.SnapshotID))

				got, _, err := b.DescribeDirectories(ctx, []string{created.DirectoryID}, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.Equal(t, DirectoryStageRestoring, got[0].Stage,
					"RestoreFromSnapshot must set Restoring immediately")

				if tt.wait > 0 {
					time.Sleep(tt.wait)
				}

				got, _, err = b.DescribeDirectories(ctx, []string{created.DirectoryID}, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.Equal(t, tt.wantStage, got[0].Stage)
			})
		})
	}
}

// TestDirectoryLifecycle_ClosedBackendCancelsPendingTransition verifies Close
// stops the worker.Group so a directory-creation transition scheduled before
// Close never fires afterward, even once real/fake time passes its deadline
// -- the leak/shutdown-race this backend used to have with a bare
// `go func() { time.Sleep(...); ... }()`.
func TestDirectoryLifecycle_ClosedBackendCancelsPendingTransition(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		b := NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")

		created, err := b.CreateDirectory(
			ctx, "corp.example.com", "CORP", "", "Admin1234!", DirectorySizeSmall, "", nil, nil,
		)
		require.NoError(t, err)

		b.Close()

		time.Sleep(3 * directoryLifecycleDelay)

		got, _, err := b.DescribeDirectories(ctx, []string{created.DirectoryID}, 0, "")
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, DirectoryStageRequested, got[0].Stage,
			"Close must cancel the pending transition so the stage never advances past shutdown")
	})
}
