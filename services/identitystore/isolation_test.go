package identitystore //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestIdentityStoreRegionIsolation proves that same-named resources created in
// two different regions are fully isolated: each region sees only its own
// resources, and deleting in one region leaves the other untouched.
func TestIdentityStoreRegionIsolation(t *testing.T) {
	t.Parallel()

	const (
		storeID  = "d-9999000000"
		eastUser = "alice"
		westUser = "alice"
		eastGrp  = "admins"
		westGrp  = "admins"
	)

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := isCtxRegion("us-east-1")
	ctxWest := isCtxRegion("us-west-2")

	// 1. Create a user with the SAME UserName in both regions.
	eastUser1, err := backend.CreateUser(ctxEast, storeID, &CreateUserRequest{UserName: eastUser})
	require.NoError(t, err)

	westUser1, err := backend.CreateUser(ctxWest, storeID, &CreateUserRequest{UserName: westUser})
	require.NoError(t, err)

	// IDs must differ even though UserNames match.
	assert.NotEqual(t, eastUser1.UserID, westUser1.UserID)

	// 2. Each region lists only its own user.
	eastUsers := backend.ListUsers(ctxEast, storeID)
	require.Len(t, eastUsers, 1)
	assert.Equal(t, eastUser1.UserID, eastUsers[0].UserID)

	westUsers := backend.ListUsers(ctxWest, storeID)
	require.Len(t, westUsers, 1)
	assert.Equal(t, westUser1.UserID, westUsers[0].UserID)

	// 3. GetUserID by username is region-scoped.
	uid, err := backend.GetUserID(ctxEast, storeID, "username", eastUser)
	require.NoError(t, err)
	assert.Equal(t, eastUser1.UserID, uid)

	uid, err = backend.GetUserID(ctxWest, storeID, "username", westUser)
	require.NoError(t, err)
	assert.Equal(t, westUser1.UserID, uid)

	// 4. Create a group with the SAME DisplayName in both regions.
	eastGrp1, err := backend.CreateGroup(ctxEast, storeID, &CreateGroupRequest{DisplayName: eastGrp})
	require.NoError(t, err)

	westGrp1, err := backend.CreateGroup(ctxWest, storeID, &CreateGroupRequest{DisplayName: westGrp})
	require.NoError(t, err)

	assert.NotEqual(t, eastGrp1.GroupID, westGrp1.GroupID)

	// 5. Memberships are region-scoped.
	eastMem, err := backend.CreateGroupMembership(
		ctxEast, storeID, eastGrp1.GroupID, MemberID{UserID: eastUser1.UserID},
	)
	require.NoError(t, err)

	westMem, err := backend.CreateGroupMembership(
		ctxWest, storeID, westGrp1.GroupID, MemberID{UserID: westUser1.UserID},
	)
	require.NoError(t, err)

	assert.NotEqual(t, eastMem.MembershipID, westMem.MembershipID)

	eastMems := backend.ListGroupMemberships(ctxEast, storeID, eastGrp1.GroupID)
	require.Len(t, eastMems, 1)

	westMems := backend.ListGroupMemberships(ctxWest, storeID, westGrp1.GroupID)
	require.Len(t, westMems, 1)

	// 6. IsMemberInGroups is region-scoped.
	eastResults := backend.IsMemberInGroups(
		ctxEast, storeID, MemberID{UserID: eastUser1.UserID}, []string{eastGrp1.GroupID},
	)
	require.Len(t, eastResults, 1)
	assert.True(t, eastResults[0].MembershipExists)

	// East user is not visible in west region.
	westCrossResults := backend.IsMemberInGroups(
		ctxWest, storeID, MemberID{UserID: eastUser1.UserID}, []string{westGrp1.GroupID},
	)
	require.Len(t, westCrossResults, 1)
	assert.False(t, westCrossResults[0].MembershipExists)

	// 7. Deleting the user in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteUser(ctxEast, storeID, eastUser1.UserID))

	eastAfterDelete := backend.ListUsers(ctxEast, storeID)
	assert.Empty(t, eastAfterDelete)

	westAfterDelete := backend.ListUsers(ctxWest, storeID)
	require.Len(t, westAfterDelete, 1)
	assert.Equal(t, westUser1.UserID, westAfterDelete[0].UserID)
}

// TestIdentityStoreDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestIdentityStoreDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	u, err := backend.CreateUser(context.Background(), "d-fallback", &CreateUserRequest{UserName: "bob"})
	require.NoError(t, err)

	// Reading via the explicit default region sees the user.
	users := backend.ListUsers(isCtxRegion("eu-central-1"), "d-fallback")
	require.Len(t, users, 1)
	assert.Equal(t, u.UserID, users[0].UserID)

	// A different region sees nothing.
	other := backend.ListUsers(isCtxRegion("ap-south-1"), "d-fallback")
	assert.Empty(t, other)
}
