package appstream

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly
// once, here, as a *store.Table[T] on b.registry. See pkgs/store's package
// doc for the underlying primitive, and services/sesv2/services/codebuild
// for the clean-direct-register template this file follows throughout --
// every table below is "clean" (no hidden/dirty field), so registry.
// SnapshotAll/RestoreAll (see persistence.go) is all Snapshot/Restore need
// for these fourteen tables.
//
// stacks, fleets, appBlocks, appBlockBuilders, applications,
// directoryConfigs, images, imageBuilders, exportTasks, and sessions were
// already flat maps keyed by their own real identity field (Name/
// DirectoryName/TaskID/ID), so each registers directly with no composite
// key.
//
// entitlements and users were already keyed by a composite string built
// from two real fields each value already carries in full
// (entitlementKey(Name, StackName); userKey(UserName, AuthenticationType)),
// so both also register directly -- the composite key is just their keyFn,
// no new field needed.
//
// themes is keyed directly by its own StackName field: AppStream allows at
// most one theme per stack, so StackName is already the natural (and only)
// identity a theme has.
//
// imagePermissions previously had no identity field of its own at all -- it
// was always looked up by an external imageName key on the plain map it
// lived in (UpdateImagePermissions/DeleteImagePermissions/
// DescribeImagePermissions). It gained a real ImageName field for this
// purpose (see storedImagePermissions in backend_image.go); a plain visible
// json tag is fine there rather than the json:"-" hidden-field + DTO
// treatment services/codecommit uses for wire-visible API types, because
// storedImagePermissions is purely internal storage -- DescribeImagePermissions
// always builds its own SharedImagePermissions response type rather than
// marshaling this one, so there is no AWS wire shape this field could leak
// into.
//
// associations, appBlockBuilderAssoc, appFleetAssoc, entitlementApps,
// softwareAssoc, and userStackAssoc (all map[string]map[string]bool) and
// tags (map[string]map[string]string) are many-to-many association/lookup
// sets with no *T value of their own for store.Table to key on, so all seven
// remain plain maps -- see the field comments on InMemoryBackend in
// backend.go and persistence.go's doc comment for the full audit. usageReport
// is a single scalar record (not a map at all), so it also stays a plain
// field.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func stackKeyFn(v *storedStack) string { return v.Name }

func fleetKeyFn(v *storedFleet) string { return v.Name }

func appBlockKeyFn(v *storedAppBlock) string { return v.Name }

func appBlockBuilderKeyFn(v *storedAppBlockBuilder) string { return v.Name }

func applicationKeyFn(v *storedApplication) string { return v.Name }

func entitlementKeyFn(v *storedEntitlement) string { return entitlementKey(v.Name, v.StackName) }

func directoryConfigKeyFn(v *storedDirectoryConfig) string { return v.DirectoryName }

func imageKeyFn(v *storedImage) string { return v.Name }

func imagePermissionsKeyFn(v *storedImagePermissions) string { return v.ImageName }

func imageBuilderKeyFn(v *storedImageBuilder) string { return v.Name }

func exportTaskKeyFn(v *storedExportImageTask) string { return v.TaskID }

func userTableKeyFn(v *storedUser) string { return userKey(v.UserName, v.AuthenticationType) }

func sessionKeyFn(v *storedSession) string { return v.ID }

func themeKeyFn(v *storedTheme) string { return v.StackName }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
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
	func(b *InMemoryBackend) { b.stacks = store.Register(b.registry, "stacks", store.New(stackKeyFn)) },
	func(b *InMemoryBackend) { b.fleets = store.Register(b.registry, "fleets", store.New(fleetKeyFn)) },
	func(b *InMemoryBackend) {
		b.appBlocks = store.Register(b.registry, "appBlocks", store.New(appBlockKeyFn))
	},
	func(b *InMemoryBackend) {
		b.appBlockBuilders = store.Register(b.registry, "appBlockBuilders", store.New(appBlockBuilderKeyFn))
	},
	func(b *InMemoryBackend) {
		b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	},
	func(b *InMemoryBackend) {
		b.entitlements = store.Register(b.registry, "entitlements", store.New(entitlementKeyFn))
	},
	func(b *InMemoryBackend) {
		b.directoryConfigs = store.Register(b.registry, "directoryConfigs", store.New(directoryConfigKeyFn))
	},
	func(b *InMemoryBackend) { b.images = store.Register(b.registry, "images", store.New(imageKeyFn)) },
	func(b *InMemoryBackend) {
		b.imagePermissions = store.Register(b.registry, "imagePermissions", store.New(imagePermissionsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.imageBuilders = store.Register(b.registry, "imageBuilders", store.New(imageBuilderKeyFn))
	},
	func(b *InMemoryBackend) {
		b.exportTasks = store.Register(b.registry, "exportTasks", store.New(exportTaskKeyFn))
	},
	func(b *InMemoryBackend) { b.users = store.Register(b.registry, "users", store.New(userTableKeyFn)) },
	func(b *InMemoryBackend) {
		b.sessions = store.Register(b.registry, "sessions", store.New(sessionKeyFn))
	},
	func(b *InMemoryBackend) { b.themes = store.Register(b.registry, "themes", store.New(themeKeyFn)) },
}
