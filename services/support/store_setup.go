package support

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T]. See pkgs/store's package doc for the underlying
// primitive.
//
// cases, attachments, checkRefreshStatuses, and checkResults each key off a
// real field the value type already carries (CaseID / AttachmentID / CheckID
// / CheckID), so they are "clean" tables: registered on b.registry so their
// Reset/Snapshot/Restore collapse into one b.registry call each.
//
// attachmentSets is a "dirty" table: AttachmentSet carried no identity field
// of its own before this conversion (its map key -- the attachment set ID --
// lived only in the surrounding map[string]*AttachmentSet, not on the value
// itself). It gained a new ID field tagged json:"-" purely so store.Table's
// keyFn can derive a key from the value; that tag means marshaling the live
// type directly would silently drop the ID, so attachmentSets is built with
// store.New only, deliberately NOT store.Register-ed onto b.registry.
// persistence.go instead builds an ephemeral DTO registry (mirroring
// services/ses's identities/configSets pattern) that gives the ID a real JSON
// tag, and Reset (backend.go) resets attachmentSets explicitly via
// resetTablesLocked.
//
// communications (map[string][]Communication, caseID -> chronological
// communications) is deliberately left a plain map: case communications are
// ORDER-SENSITIVE (chronological), and store.Table.Snapshot/All/Restore make
// no iteration-order guarantee, so converting it would silently scramble
// communication history on every Snapshot->Restore round trip.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func caseKeyFn(v *Case) string { return v.CaseID }

func attachmentKeyFn(v *Attachment) string { return v.AttachmentID }

func checkRefreshStatusKeyFn(v *TrustedAdvisorCheckRefreshStatus) string { return v.CheckID }

func checkResultKeyFn(v *TrustedAdvisorCheckResult) string { return v.CheckID }

func attachmentSetKeyFn(v *AttachmentSet) string { return v.ID }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; the "dirty" attachmentSets table is constructed alongside them
// but deliberately NOT registered -- see the file doc comment above. It must
// be called during construction only, never on every Reset(): store.Register
// panics on a duplicate name, so runtime resets go through resetTablesLocked
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.cases = store.Register(b.registry, "cases", store.New(caseKeyFn))
	b.attachments = store.Register(b.registry, "attachments", store.New(attachmentKeyFn))
	b.checkRefreshStatuses = store.Register(
		b.registry, "checkRefreshStatuses", store.New(checkRefreshStatusKeyFn),
	)
	b.checkResults = store.Register(b.registry, "checkResults", store.New(checkResultKeyFn))

	b.attachmentSets = store.New(attachmentSetKeyFn)
}
