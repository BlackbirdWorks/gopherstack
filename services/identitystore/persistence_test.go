package identitystore //nolint:testpackage // needs isCtxRegion (isolation_test.go) to build multi-region contexts.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_RestoreRejectsIncompatibleSnapshot verifies that a
// snapshot which cannot be safely decoded as the current shape -- malformed
// JSON, an explicit version mismatch, or the pre-Phase-3.3 shape (which has
// no "version" field at all and so decodes as Version == 0) -- is handled
// correctly: malformed JSON is a hard error, while every other incompatible
// shape is discarded cleanly (registry reset, no error) rather than
// partially applied.
func TestInMemoryBackend_RestoreRejectsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "malformed JSON is an error",
			data:    []byte("not-valid-json"),
			wantErr: true,
		},
		{
			name:    "explicit version mismatch is discarded, not an error",
			data:    []byte(`{"version":999,"tables":{}}`),
			wantErr: false,
		},
		{
			name: "pre-Phase-3.3 shape with no version field decodes as 0, is discarded",
			data: []byte(
				`{"users":{"us-east-1":{"u-1":{"UserId":"u-1","IdentityStoreId":"d-1","UserName":"old"}}}}`,
			),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			_, seedErr := b.CreateUser(t.Context(), "d-seed", &CreateUserRequest{UserName: "seed"})
			require.NoError(t, seedErr)

			err := b.Restore(t.Context(), tt.data)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, b.ListUsers(t.Context(), "d-seed"))
		})
	}
}

// seedFullState populates b with one user, one group, and one membership
// between them, plus a second user in a different AWS region under the same
// UserName to prove the hidden `region` field each DTO carries round-trips
// through Snapshot/Restore (see regionalDTO in persistence.go). It returns
// the created resources for later assertions.
func seedFullState(t *testing.T, b *InMemoryBackend) (*User, *Group, *GroupMembership, *User) {
	t.Helper()

	const storeID = "d-1111111111"

	user, err := b.CreateUser(isCtxRegion("us-east-1"), storeID, &CreateUserRequest{
		UserName:    "alice",
		DisplayName: "Alice Example",
		Emails:      []Email{{Value: "alice@example.com", Primary: true}},
		ExternalIDs: []ExternalID{{Issuer: "okta", ID: "ext-1"}},
	})
	require.NoError(t, err)

	group, err := b.CreateGroup(isCtxRegion("us-east-1"), storeID, &CreateGroupRequest{
		DisplayName: "engineering",
		Description: "eng team",
	})
	require.NoError(t, err)

	membership, err := b.CreateGroupMembership(
		isCtxRegion("us-east-1"), storeID, group.GroupID, MemberID{UserID: user.UserID},
	)
	require.NoError(t, err)

	// Same UserName ("alice") in a different region under the same store ID
	// -- proves region isolation (and the hidden region field) survive the
	// round trip, not just the resource data itself.
	westUser, err := b.CreateUser(isCtxRegion("us-west-2"), storeID, &CreateUserRequest{
		UserName: "alice",
	})
	require.NoError(t, err)

	return user, group, membership, westUser
}

// TestInMemoryBackend_SnapshotRestoreFullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (users, groups, memberships) plus their secondary
// indexes (byUserName/byPrimaryEmail, byDisplayName, byGroup/byMember/
// byGroupMember) and the region isolation the hidden `region` field on each
// value carries.
func TestInMemoryBackend_SnapshotRestoreFullState(t *testing.T) {
	t.Parallel()

	const storeID = "d-1111111111"

	original := NewInMemoryBackend("111122223333", "us-east-1")
	user, group, membership, westUser := seedFullState(t, original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	t.Run("scalars", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "us-east-1", fresh.Region())
	})

	t.Run("users table plus byUserName/byPrimaryEmail indexes", func(t *testing.T) {
		t.Parallel()

		got, err := fresh.DescribeUser(isCtxRegion("us-east-1"), storeID, user.UserID)
		require.NoError(t, err)
		assert.Equal(t, "alice", got.UserName)
		assert.Equal(t, "Alice Example", got.DisplayName)

		uid, err := fresh.GetUserID(isCtxRegion("us-east-1"), storeID, "username", "alice")
		require.NoError(t, err)
		assert.Equal(t, user.UserID, uid)

		uid, err = fresh.GetUserID(isCtxRegion("us-east-1"), storeID, "emails.value", "alice@example.com")
		require.NoError(t, err)
		assert.Equal(t, user.UserID, uid)

		uid, err = fresh.GetUserID(isCtxRegion("us-east-1"), storeID, "externalid", "okta\x00ext-1")
		require.NoError(t, err)
		assert.Equal(t, user.UserID, uid)

		// The byUserName uniqueness index must still reject a duplicate after
		// restore, proving it was rebuilt (not left empty) by Table.Restore.
		_, err = fresh.CreateUser(isCtxRegion("us-east-1"), storeID, &CreateUserRequest{UserName: "alice"})
		require.ErrorIs(t, err, ErrConflict)
	})

	t.Run("groups table plus byDisplayName index", func(t *testing.T) {
		t.Parallel()

		got, err := fresh.DescribeGroup(isCtxRegion("us-east-1"), storeID, group.GroupID)
		require.NoError(t, err)
		assert.Equal(t, "engineering", got.DisplayName)

		gid, err := fresh.GetGroupID(isCtxRegion("us-east-1"), storeID, "displayName", "engineering")
		require.NoError(t, err)
		assert.Equal(t, group.GroupID, gid)
	})

	t.Run("memberships table plus byGroup/byMember/byGroupMember indexes", func(t *testing.T) {
		t.Parallel()

		got, err := fresh.DescribeGroupMembership(isCtxRegion("us-east-1"), storeID, membership.MembershipID)
		require.NoError(t, err)
		assert.Equal(t, group.GroupID, got.GroupID)

		byGroup := fresh.ListGroupMemberships(isCtxRegion("us-east-1"), storeID, group.GroupID)
		require.Len(t, byGroup, 1)
		assert.Equal(t, membership.MembershipID, byGroup[0].MembershipID)

		byMember := fresh.ListGroupMembershipsForMember(
			isCtxRegion("us-east-1"), storeID, MemberID{UserID: user.UserID},
		)
		require.Len(t, byMember, 1)
		assert.Equal(t, membership.MembershipID, byMember[0].MembershipID)

		mid, err := fresh.GetGroupMembershipID(
			isCtxRegion("us-east-1"), storeID, group.GroupID, MemberID{UserID: user.UserID},
		)
		require.NoError(t, err)
		assert.Equal(t, membership.MembershipID, mid)

		results := fresh.IsMemberInGroups(
			isCtxRegion("us-east-1"), storeID, MemberID{UserID: user.UserID}, []string{group.GroupID},
		)
		require.Len(t, results, 1)
		assert.True(t, results[0].MembershipExists)
	})

	t.Run("region isolation survives the round trip", func(t *testing.T) {
		t.Parallel()

		eastUsers := fresh.ListUsers(isCtxRegion("us-east-1"), storeID)
		require.Len(t, eastUsers, 1)
		assert.Equal(t, user.UserID, eastUsers[0].UserID)

		westUsers := fresh.ListUsers(isCtxRegion("us-west-2"), storeID)
		require.Len(t, westUsers, 1)
		assert.Equal(t, westUser.UserID, westUsers[0].UserID)
	})
}
