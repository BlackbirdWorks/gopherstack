package workspaces

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc for
// the underlying primitive, and services/sesv2/services/medialive/
// services/codebuild for the template this mirrors.
//
// Every one of the 12 tables below is "clean": each value type already
// carries its own identity as a real (non-json:"-") field --
// storedWorkspace.WorkspaceID, storedIpGroup.GroupID,
// storedConnAlias.AliasID, storedCustomBundle.BundleID,
// storedImage.ImageID, storedPool.PoolID, storedPoolSession.SessionID,
// storedConnectAddIn.AddInID, storedClientBranding.ResourceID,
// storedAccountLink.LinkID, storedApplication.AppID,
// storedDirSettings.DirectoryID -- so none need the "dirty" DTO-registry
// treatment (see services/codecommit for that pattern) or a composite/
// region-nested key (see services/emr): all 12 register directly on
// b.registry, and persistence.go drives every one of them through
// b.registry.SnapshotAll() / RestoreAll(). None of the value types nest
// under a parent, so no secondary store.Index is needed either.
//
// Left as plain maps (not store.Table-backed) -- see the field comments on
// InMemoryBackend in backend.go for the full audit of which are persisted
// (directly in backendSnapshot, not via b.registry) and which remain
// ephemeral:
//   - tags (map[string]map[string]string) and directoryIpGroups
//     (map[string]map[string]struct{}) are persisted directly in
//     backendSnapshot (see persistence.go); values are plain maps, not *T,
//     so store.Table does not apply.
//   - imagePermissions (map[string]map[string]bool) and appAssociations
//     (map[string]map[string]struct{}) remain unpersisted -- ephemeral, same
//     "not *T" reason.
//   - clientProperties (map[string]storedClientProps): values are not
//     pointers, and storedClientProps carries no identity field of its own
//     to key a store.Table by; also unpersisted.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func workspaceKeyFn(v *storedWorkspace) string { return v.WorkspaceID }

func ipGroupKeyFn(v *storedIpGroup) string { return v.GroupID }

func connAliasKeyFn(v *storedConnAlias) string { return v.AliasID }

func customBundleKeyFn(v *storedCustomBundle) string { return v.BundleID }

func imageKeyFn(v *storedImage) string { return v.ImageID }

func poolKeyFn(v *storedPool) string { return v.PoolID }

func poolSessionKeyFn(v *storedPoolSession) string { return v.SessionID }

func connectAddInKeyFn(v *storedConnectAddIn) string { return v.AddInID }

func clientBrandingKeyFn(v *storedClientBranding) string { return v.ResourceID }

func accountLinkKeyFn(v *storedAccountLink) string { return v.LinkID }

func applicationKeyFn(v *storedApplication) string { return v.AppID }

func dirSettingsKeyFn(v *storedDirSettings) string { return v.DirectoryID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.workspaces = store.Register(b.registry, "workspaces", store.New(workspaceKeyFn))
	},
	func(b *InMemoryBackend) {
		b.ipGroups = store.Register(b.registry, "ipGroups", store.New(ipGroupKeyFn))
	},
	func(b *InMemoryBackend) {
		b.connAliases = store.Register(b.registry, "connAliases", store.New(connAliasKeyFn))
	},
	func(b *InMemoryBackend) {
		b.customBundles = store.Register(b.registry, "customBundles", store.New(customBundleKeyFn))
	},
	func(b *InMemoryBackend) {
		b.images = store.Register(b.registry, "images", store.New(imageKeyFn))
	},
	func(b *InMemoryBackend) {
		b.pools = store.Register(b.registry, "pools", store.New(poolKeyFn))
	},
	func(b *InMemoryBackend) {
		b.poolSessions = store.Register(b.registry, "poolSessions", store.New(poolSessionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.connectAddIns = store.Register(b.registry, "connectAddIns", store.New(connectAddInKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clientBranding = store.Register(b.registry, "clientBranding", store.New(clientBrandingKeyFn))
	},
	func(b *InMemoryBackend) {
		b.accountLinks = store.Register(b.registry, "accountLinks", store.New(accountLinkKeyFn))
	},
	func(b *InMemoryBackend) {
		b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dirSettings = store.Register(b.registry, "dirSettings", store.New(dirSettingsKeyFn))
	},
}
