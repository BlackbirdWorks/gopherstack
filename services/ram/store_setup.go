package ram

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2 (commit
// 12e611a4, data-driven registration slice), services/sqs (commit 0f09d77c,
// DTO-registry for types that can't round-trip a direct JSON marshal), and
// services/sesv2 (commit adjacent, clean direct-register + composite-key +
// Index pattern). See pkgs/store's package doc for the underlying primitive.
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- ResourceShare.ARN, Permission.ARN, and
// ResourceShareInvitation.InvitationARN were already set consistently at
// every write site before this refactor. So all three tables below are
// "clean" tables registered directly on b.registry, and persistence.go
// drives every one of them through b.registry.SnapshotAll() / RestoreAll().
// None need the ses-style "dirty" DTO-registry treatment.
//
// Left as plain fields (not store.Table-backed):
//   - sharePermissions (map[string]map[string]int32): values are plain
//     int32s, not *T, so it does not fit store.Table's keyed-by-identity-value
//     shape.
//   - associations ([]*ResourceShareAssociation): a slice, not a map. There
//     is no field on ResourceShareAssociation that is a stable, universally
//     unique primary key -- multiple associations can exist for the same
//     (ResourceShareARN, AssociatedEntity) pair depending on association
//     type, and DisassociateResourceShare removes entries from the slice
//     outright rather than updating them in place. Both remain persisted
//     directly on backendSnapshot exactly as before.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func resourceShareKeyFn(v *ResourceShare) string { return v.ARN }

func permissionKeyFn(v *Permission) string { return v.ARN }

func invitationKeyFn(v *ResourceShareInvitation) string { return v.InvitationARN }

func replaceWorkKeyFn(v *ReplacePermissionAssociationsWork) string { return v.ID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.resourceShares = store.Register(b.registry, "resourceShares", store.New(resourceShareKeyFn))
	b.permissions = store.Register(b.registry, "permissions", store.New(permissionKeyFn))
	b.invitations = store.Register(b.registry, "invitations", store.New(invitationKeyFn))
	b.replaceWorks = store.Register(b.registry, "replaceWorks", store.New(replaceWorkKeyFn))
}
