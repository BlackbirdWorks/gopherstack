package directoryservice //nolint:testpackage // needs access to statusTransitionDelay.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTransitionBackend(t *testing.T) (*InMemoryBackend, string) {
	t.Helper()

	b := NewInMemoryBackendWithContext(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(b.Close)

	d, err := b.CreateDirectory(
		context.Background(), "corp.example.com", "CORP", "", "Admin1234!",
		DirectorySizeSmall, "", nil, nil,
	)
	require.NoError(t, err)

	return b, d.DirectoryID
}

func TestTrustStateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		direction string
		op        func(b *InMemoryBackend, trustID string) error
		wantEarly string
		wantFinal string
	}{
		{
			name: "create two way", direction: "Two-Way",
			op:        func(*InMemoryBackend, string) error { return nil },
			wantEarly: "Creating", wantFinal: "Verified",
		},
		{
			name: "create incoming", direction: "One-Way: Incoming",
			op:        func(*InMemoryBackend, string) error { return nil },
			wantEarly: "Creating", wantFinal: "Created",
		},
		{
			name: "verify", direction: "Two-Way",
			op: func(b *InMemoryBackend, id string) error {
				_, err := b.VerifyTrust(context.Background(), id)

				return err
			},
			wantEarly: "Verifying", wantFinal: "Verified",
		},
		{
			name: "update", direction: "Two-Way",
			op: func(b *InMemoryBackend, id string) error {
				_, err := b.UpdateTrust(context.Background(), id, "Enabled")

				return err
			},
			wantEarly: "Updating", wantFinal: "Updated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, dirID := newTransitionBackend(t)
				ctx := context.Background()

				id, err := b.CreateTrust(ctx, CreateTrustInput{
					DirectoryID: dirID, RemoteDomainName: "partner.example.com",
					TrustPassword: "TrustPw1!", TrustDirection: tt.direction,
				})
				require.NoError(t, err)

				if tt.wantEarly != "Creating" {
					time.Sleep(statusTransitionDelay + time.Millisecond)
					synctest.Wait()
				}

				require.NoError(t, tt.op(b, id))

				got, _, err := b.DescribeTrusts(ctx, dirID, nil, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, tt.wantEarly, got[0].TrustState)

				time.Sleep(statusTransitionDelay + time.Millisecond)
				synctest.Wait()

				got, _, err = b.DescribeTrusts(ctx, dirID, nil, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, tt.wantFinal, got[0].TrustState)
			})
		})
	}
}

func TestDeleteTrustStateTransitions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, dirID := newTransitionBackend(t)
		ctx := context.Background()

		id, err := b.CreateTrust(ctx, CreateTrustInput{
			DirectoryID: dirID, RemoteDomainName: "partner.example.com",
			TrustPassword: "TrustPw1!", TrustDirection: "Two-Way",
		})
		require.NoError(t, err)
		time.Sleep(statusTransitionDelay + time.Millisecond)
		synctest.Wait()

		_, err = b.DeleteTrust(ctx, id, false)
		require.NoError(t, err)

		got, _, err := b.DescribeTrusts(ctx, dirID, nil, 0, "")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Deleting", got[0].TrustState)

		time.Sleep(statusTransitionDelay + time.Millisecond)
		synctest.Wait()

		got, _, err = b.DescribeTrusts(ctx, dirID, nil, 0, "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestSnapshotStatusTransitions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, dirID := newTransitionBackend(t)
		ctx := context.Background()

		snap, err := b.CreateSnapshot(ctx, dirID, "nightly")
		require.NoError(t, err)

		got, _, err := b.DescribeSnapshots(ctx, dirID, []string{snap.SnapshotID}, 0, "")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, SnapshotStatusCreating, got[0].Status)

		time.Sleep(statusTransitionDelay + time.Millisecond)
		synctest.Wait()

		got, _, err = b.DescribeSnapshots(ctx, dirID, []string{snap.SnapshotID}, 0, "")
		require.NoError(t, err)
		assert.Equal(t, SnapshotStatusCompleted, got[0].Status)
	})
}

func TestShareDirectoryStatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantEarly  string
		wantSettle string
	}{
		{name: "organizations", method: "ORGANIZATIONS", wantEarly: "Sharing", wantSettle: "Shared"},
		{name: "handshake", method: "HANDSHAKE", wantEarly: "PendingAcceptance", wantSettle: "PendingAcceptance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b, dirID := newTransitionBackend(t)
				ctx := context.Background()

				_, err := b.ShareDirectory(ctx, dirID, tt.method, "", "222222222222")
				require.NoError(t, err)

				got, _, err := b.DescribeSharedDirectories(ctx, dirID, nil, 0, "")
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, tt.wantEarly, got[0].ShareStatus)

				time.Sleep(statusTransitionDelay + time.Millisecond)
				synctest.Wait()

				got, _, err = b.DescribeSharedDirectories(ctx, dirID, nil, 0, "")
				require.NoError(t, err)
				assert.Equal(t, tt.wantSettle, got[0].ShareStatus)
			})
		})
	}
}

func TestUpdateSettingsTransitions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, dirID := newTransitionBackend(t)
		ctx := context.Background()

		_, err := b.UpdateSettings(ctx, dirID, []DirectorySetting{{Name: "TLS_1_0", Value: "Disable"}})
		require.NoError(t, err)

		got, _, err := b.DescribeSettings(ctx, dirID, "", "")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Requested", got[0].Status)
		assert.Empty(t, got[0].AppliedValue)
		assert.Equal(t, map[string]string{"us-east-1": "Requested"}, got[0].RegionStatuses)

		time.Sleep(2*statusTransitionDelay + time.Millisecond)
		synctest.Wait()

		got, _, err = b.DescribeSettings(ctx, dirID, "", "")
		require.NoError(t, err)
		assert.Equal(t, "Updated", got[0].Status)
		assert.Equal(t, "Disable", got[0].AppliedValue)

		_, err = b.UpdateSettings(ctx, dirID, []DirectorySetting{{Name: "TLS_1_0", Value: "Enable"}})
		require.NoError(t, err)
		time.Sleep(2*statusTransitionDelay + time.Millisecond)
		synctest.Wait()

		got, _, err = b.DescribeSettings(ctx, dirID, "", "")
		require.NoError(t, err)
		assert.Equal(t, "Updated", got[0].Status)
		assert.Equal(t, "Enable", got[0].AppliedValue)
	})
}

func TestUpdateDirectorySetupSnapshotBeforeUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		snapshot  bool
		wantCount int
	}{
		{name: "requested", snapshot: true, wantCount: 1},
		{name: "not requested", snapshot: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, dirID := newTransitionBackend(t)
			ctx := context.Background()

			require.NoError(t, b.UpdateDirectorySetup(ctx, dirID, DirectorySetupUpdate{
				UpdateType: string(UpdateTypeSize), DirectorySize: string(DirectorySizeLarge),
				CreateSnapshotBeforeUpdate: tt.snapshot,
			}))

			got, _, err := b.DescribeSnapshots(ctx, dirID, nil, 0, "")
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)

			if tt.snapshot {
				assert.Equal(t, SnapshotTypeAuto, got[0].Type)
			}
		})
	}
}

func TestReplicaRegionVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(ctx context.Context, b *InMemoryBackend, dirID string) error
		name string
	}{
		{call: func(ctx context.Context, b *InMemoryBackend, id string) error {
			_, _, err := b.ListTagsForResource(ctx, id, 0, "")

			return err
		}, name: "list tags"},
		{call: func(ctx context.Context, b *InMemoryBackend, id string) error {
			got, _, err := b.DescribeRegions(ctx, id, "", "")
			if err == nil {
				require.Len(t, got, 1)
			}

			return err
		}, name: "describe regions"},
		{call: func(ctx context.Context, b *InMemoryBackend, id string) error {
			_, _, err := b.DescribeDirectories(ctx, []string{id}, 0, "")

			return err
		}, name: "describe directories"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, dirID := newTransitionBackend(t)
			primary := context.Background()
			replica := context.WithValue(primary, regionContextKey{}, "us-west-2")
			stranger := context.WithValue(primary, regionContextKey{}, "eu-west-1")

			require.Error(t, tt.call(replica, b, dirID), "no replica yet")
			require.NoError(t, b.AddRegion(primary, dirID, "us-west-2", nil))
			require.NoError(t, tt.call(replica, b, dirID))
			require.Error(t, tt.call(stranger, b, dirID))
		})
	}
}
