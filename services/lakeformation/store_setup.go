package lakeformation

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// backend-level map[K]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2
// (data-driven registration slice), services/ses (DTO-registry for types
// that can't round-trip a direct JSON marshal), and services/sesv2 /
// services/medialive (clean direct registration + composite-key
// flattening). See pkgs/store's package doc for the underlying primitive.
//
// Five tables carry their own identity already, as real (non-json:"-")
// fields, and are registered directly on b.registry as "clean" tables:
//
//   - resources (ResourceInfo.ResourceArn) and identityCenterConfigs
//     (IdentityCenterConfiguration.CatalogID) are single-field keys.
//   - lfTags, dataCellsFilters, and lfTagExpressions were previously keyed
//     by a small unexported struct (lfTagKey / dataCellsFilterKey /
//     lfTagExpressionKey) built purely from fields the value type already
//     carries (CatalogID+TagKey, TableCatalogID+DatabaseName+TableName+Name,
//     CatalogID+Name respectively). Those struct types are gone; each is
//     flattened into a single composite string key via the *KeyStr helper
//     below, mirroring services/sesv2's eventDestinationKey/contactKey
//     pattern. Access sites call the *KeyStr helper directly wherever the
//     old code built the key struct.
//
// transactions is "dirty": transactionInfo carried no field naming its own
// transaction ID before this refactor (the ID lived only as the map key), so
// it gained an ID field tagged json:"-" purely so store.Table's keyFn can
// derive a key from the value -- see transactionInfo's doc comment in
// transactions.go. It is NOT registered on b.registry: persistence.go instead
// round-trips it through a dedicated transactionSnapshot DTO that carries
// the ID as a real JSON field, mirroring services/ses's identities table.
//
// permissionsMap is neither "clean" nor "dirty": it is a derived O(1) lookup
// cache over b.permissionsList (the sort-on-write slice that remains the
// source of truth for GrantPermissions/RevokePermissions ordering and for
// persistence), keyed by the pre-existing permissionKey helper (already a
// pure function of PermissionEntry.Principal/.Resource, defined in
// permissions.go). It is rebuilt from b.permissionsList after every
// Restore/Reset rather than snapshotted independently -- see persistence.go.
//
// Left as plain maps (not store.Table-backed), each audited against the
// pre-refactor persistence.go for its persisted status:
//   - resourceLFTags (map[string][]LFTagPair): slice-valued, not *T. Was
//     persisted before -- still persisted, unchanged.
//   - queries (map[string]string): string-valued, not *T. Was persisted
//     before -- still persisted, unchanged.
//   - tableStorageOptimizers (map[string][]StorageOptimizer): slice-valued,
//     not *T. Was persisted before -- still persisted, unchanged.
//   - tableObjects (map[string][]PartitionedTableObjectsList): slice-valued,
//     not *T. Was NOT persisted before (absent from the old
//     backendSnapshot) -- still not persisted, unchanged.
//
// dataLakeSettings (*DataLakeSettings) is a single pointer, not a
// collection, so it is untouched by this refactor. permissionsList
// ([]*PermissionEntry) and lakeFormationOptIns ([]*LFOptIn) are already
// plain slices, not maps, so neither fits store.Table's keyed-collection
// shape; both are untouched.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func resourceInfoKeyFn(v *ResourceInfo) string { return v.ResourceArn }

func identityCenterConfigKeyFn(v *IdentityCenterConfiguration) string { return v.CatalogID }

// lfTagKeyStr builds the composite "<catalogID>|<tagKey>" key shared by
// every LF tag. Access sites use this directly so the exact same key is used
// for Put/Get/Has/Delete.
func lfTagKeyStr(catalogID, tagKey string) string { return catalogID + "|" + tagKey }

func lfTagKeyFn(v *LFTag) string { return lfTagKeyStr(v.CatalogID, v.TagKey) }

// dataCellsFilterKeyStr builds the composite
// "<tableCatalogID>|<databaseName>|<tableName>|<name>" key shared by every
// data cells filter. Access sites use this directly so the exact same key is
// used for Put/Get/Has/Delete.
func dataCellsFilterKeyStr(tableCatalogID, databaseName, tableName, name string) string {
	return tableCatalogID + "|" + databaseName + "|" + tableName + "|" + name
}

func dataCellsFilterKeyFn(v *DataCellsFilter) string {
	return dataCellsFilterKeyStr(v.TableCatalogID, v.DatabaseName, v.TableName, v.Name)
}

// lfTagExpressionKeyStr builds the composite "<catalogID>|<name>" key shared
// by every LF-tag expression. Access sites use this directly so the exact
// same key is used for Put/Get/Has/Delete.
func lfTagExpressionKeyStr(catalogID, name string) string { return catalogID + "|" + name }

func lfTagExpressionKeyFn(v *LFTagExpression) string {
	return lfTagExpressionKeyStr(v.CatalogID, v.Name)
}

func transactionKeyFn(v *transactionInfo) string { return v.ID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. "Clean" tables are
// registered on b.registry; the "dirty" transactions table and the
// permissionsMap derived cache are constructed alongside them but
// deliberately NOT registered -- see the file doc comment above. It must be
// called during construction only, never on every Reset(): store.Register
// panics on a duplicate name, so runtime resets go through
// resetTablesLocked (store.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.resources = store.Register(b.registry, "resources", store.New(resourceInfoKeyFn))
	b.identityCenterConfigs = store.Register(
		b.registry, "identityCenterConfigs", store.New(identityCenterConfigKeyFn),
	)
	b.lfTags = store.Register(b.registry, "lfTags", store.New(lfTagKeyFn))
	b.dataCellsFilters = store.Register(b.registry, "dataCellsFilters", store.New(dataCellsFilterKeyFn))
	b.lfTagExpressions = store.Register(b.registry, "lfTagExpressions", store.New(lfTagExpressionKeyFn))

	b.transactions = store.New(transactionKeyFn)
	b.permissionsMap = store.New(permissionKey)
}
