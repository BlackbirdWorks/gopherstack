package identitystore

// Code in this file supports Phase 3.3 of the datalayer refactor: the three
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*User, with IdentityStoreID carried as
// a field for per-store filtering within a region) are each replaced by a
// single flat *store.Table[T], keyed by the composite "region#id" string
// (see regionKey in backend.go) -- the region-qualified-table pattern
// services/emr and services/mwaa use, with region standing in for what
// services/workmail calls org.
//
// users additionally gets byUserName and byPrimaryEmail secondary indexes
// (each keyed by "region#storeID#value") replacing the old per-region
// usersByName/usersByEmail reverse-lookup maps -- both are uniqueness
// constraints enforced by the create/rename call sites (see CreateUser,
// validateUsernameRename), so each index group holds at most one *User,
// mirroring services/cloudtrail's trailsByARN/channelsByName treatment of a
// unique-name lookup as a store.Index rather than a raw map.
//
// groups gets an analogous byDisplayName index replacing groupsByName.
//
// memberships gets three indexes: byGroup (ListGroupMemberships),
// byMember (ListGroupMembershipsForMember, and DeleteUser's cascade delete),
// and byGroupMember (the CreateGroupMembership duplicate check,
// GetGroupMembershipID, and IsMemberInGroups) -- together replacing the old
// membershipKeys and membershipsByUser reverse-lookup maps. All three are
// pure functions of a GroupMembership's own fields, so Table.Put/Delete keep
// them consistent automatically.
//
// users, groups, and memberships all carry a hidden (unexported) `region`
// field (see backend.go) that plain JSON marshaling -- both the wire
// response and store.Table.Snapshot's json.Marshal -- cannot see, so none of
// the three are round-tripped through b.registry.SnapshotAll/RestoreAll
// directly; persistence.go builds a separate ephemeral DTO registry that
// carries region alongside each value instead (mirroring services/emr's
// regionalDTO[V]). They ARE registered on b.registry (see registerAllTables
// below) so that Reset and the persistence version-mismatch path can still
// reset them via one b.registry.ResetAll() call.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func userKeyFn(v *User) string           { return regionKey(v.region, v.UserID) }
func userStoreIndexKeyFn(v *User) string { return storeKey(v.region, v.IdentityStoreID) }
func userNameIndexKeyFn(v *User) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + v.UserName
}
func userEmailIndexKeyFn(v *User) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + userPrimaryEmail(v.Emails)
}

func groupKeyFn(v *Group) string           { return regionKey(v.region, v.GroupID) }
func groupStoreIndexKeyFn(v *Group) string { return storeKey(v.region, v.IdentityStoreID) }
func groupDisplayNameIndexKeyFn(v *Group) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + v.DisplayName
}

func membershipKeyFn(v *GroupMembership) string { return regionKey(v.region, v.MembershipID) }

func membershipGroupIndexKeyFn(v *GroupMembership) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + v.GroupID
}

func membershipMemberIndexKeyFn(v *GroupMembership) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + v.MemberID.UserID
}

func membershipGroupMemberIndexKeyFn(v *GroupMembership) string {
	return storeKey(v.region, v.IdentityStoreID) + "#" + v.GroupID + "#" + v.MemberID.UserID
}

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.users = store.Register(b.registry, "users", store.New(userKeyFn))
	b.usersByStore = b.users.AddIndex("byStore", userStoreIndexKeyFn)
	b.usersByUserName = b.users.AddIndex("byUserName", userNameIndexKeyFn)
	b.usersByPrimaryEmail = b.users.AddIndex("byPrimaryEmail", userEmailIndexKeyFn)

	b.groups = store.Register(b.registry, "groups", store.New(groupKeyFn))
	b.groupsByStore = b.groups.AddIndex("byStore", groupStoreIndexKeyFn)
	b.groupsByDisplayName = b.groups.AddIndex("byDisplayName", groupDisplayNameIndexKeyFn)

	b.memberships = store.Register(b.registry, "memberships", store.New(membershipKeyFn))
	b.membershipsByGroup = b.memberships.AddIndex("byGroup", membershipGroupIndexKeyFn)
	b.membershipsByMember = b.memberships.AddIndex("byMember", membershipMemberIndexKeyFn)
	b.membershipsByGroupMember = b.memberships.AddIndex("byGroupMember", membershipGroupMemberIndexKeyFn)
}
