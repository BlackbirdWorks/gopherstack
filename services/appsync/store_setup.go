package appsync

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c)
// conversions this follows.
//
// # Nested collections
//
// datasources, resolvers, functions, types, and channelNamespaces were
// previously nested per-parent maps (map[apiID]map[localKey]*T). store.Table
// has no notion of nesting, so each becomes a single flat table keyed by a
// composite "<apiID>#<localKey>" string, with a secondary [store.Index]
// grouping by API ID for the "all children of API X" lookups the nested maps
// used to answer directly (see services/apigateway's store_setup.go for the
// same pattern). Unlike apigateway's Resource/Stage/etc, whose parent-ID
// field is internal-only (tagged json:"-"), every child value type here
// already carries its own APIID field as a REAL, wire-serialized identity
// field (e.g. DataSource.APIID is returned to callers as "apiId"), so these
// composite tables round-trip through JSON with no information loss and no
// DTO/dirty-table split is needed -- they are all "clean" tables, registered
// directly on b.registry.
//
// # What is NOT converted here, and why
//
//   - apiKeys (map[apiID]map[keyID]*APIKey): APIKey carries no APIID field of
//     its own (only ID/Description/Expires/Deletes), so there is no pure
//     func(*APIKey) string that could reproduce the "<apiID>#<keyID>"
//     composite key from the value alone -- store.Table's keyFn contract
//     requires the key to be derived purely from the value. Same exclusion
//     class as EC2's instanceIMDSOptions/verifiedAccessEndpointPolicies
//     (value has no identity field naming its parent). Left as a plain
//     nested map, reset by hand in InMemoryBackend.Reset.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func apisKeyFn(v *GraphqlAPI) string { return v.APIID }

func schemasKeyFn(v *Schema) string { return v.APIID }

// datasourceKey builds the composite "<apiID>#<name>" key shared by every
// data source access site, so Put/Get/Delete always agree on the same key.
func datasourceKey(apiID, name string) string { return apiID + "#" + name }
func datasourcesKeyFn(v *DataSource) string   { return datasourceKey(v.APIID, v.Name) }
func datasourceAPIIndexKeyFn(v *DataSource) string {
	return v.APIID
}

// resolverTableKey builds the composite "<apiID>#<TypeName.FieldName>" key
// for the resolvers table, reusing resolverKey (backend.go) for the
// "TypeName.FieldName" portion so both halves of the key are built the same
// way everywhere.
func resolverTableKey(apiID, typeName, fieldName string) string {
	return apiID + "#" + resolverKey(typeName, fieldName)
}
func resolversKeyFn(v *Resolver) string { return resolverTableKey(v.APIID, v.TypeName, v.FieldName) }
func resolverAPIIndexKeyFn(v *Resolver) string {
	return v.APIID
}

func apiCachesKeyFn(v *APICache) string { return v.APIID }

func functionKey(apiID, functionID string) string { return apiID + "#" + functionID }
func functionsKeyFn(v *Function) string           { return functionKey(v.APIID, v.FunctionID) }
func functionAPIIndexKeyFn(v *Function) string {
	return v.APIID
}

func apiTypeKey(apiID, name string) string { return apiID + "#" + name }
func typesKeyFn(v *APIType) string         { return apiTypeKey(v.APIID, v.Name) }
func typeAPIIndexKeyFn(v *APIType) string {
	return v.APIID
}

func domainNamesKeyFn(v *DomainName) string         { return v.DomainName }
func apiAssociationsKeyFn(v *APIAssociation) string { return v.DomainName }
func eventAPIsKeyFn(v *API) string                  { return v.APIID }

func channelNamespaceKey(apiID, name string) string { return apiID + "#" + name }
func channelNamespacesKeyFn(v *ChannelNamespace) string {
	return channelNamespaceKey(v.APIID, v.Name)
}
func channelNamespaceAPIIndexKeyFn(v *ChannelNamespace) string {
	return v.APIID
}

func sourceAssocsKeyFn(v *SourceAPIAssociation) string { return v.AssociationID }

func introspectionsKeyFn(v *DataSourceIntrospection) string { return v.IntrospectionID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (plus any secondary store.AddIndex) to the concrete field and value
// type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.apis = store.Register(b.registry, "apis", store.New(apisKeyFn))
	},
	func(b *InMemoryBackend) {
		b.schemas = store.Register(b.registry, "schemas", store.New(schemasKeyFn))
	},
	func(b *InMemoryBackend) {
		b.datasources = store.Register(b.registry, "datasources", store.New(datasourcesKeyFn))
		b.datasourcesByAPI = b.datasources.AddIndex("byAPI", datasourceAPIIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.resolvers = store.Register(b.registry, "resolvers", store.New(resolversKeyFn))
		b.resolversByAPI = b.resolvers.AddIndex("byAPI", resolverAPIIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.apiCaches = store.Register(b.registry, "apiCaches", store.New(apiCachesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.functions = store.Register(b.registry, "functions", store.New(functionsKeyFn))
		b.functionsByAPI = b.functions.AddIndex("byAPI", functionAPIIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.types = store.Register(b.registry, "types", store.New(typesKeyFn))
		b.typesByAPI = b.types.AddIndex("byAPI", typeAPIIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.domainNames = store.Register(b.registry, "domainNames", store.New(domainNamesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.apiAssociations = store.Register(b.registry, "apiAssociations", store.New(apiAssociationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.eventAPIs = store.Register(b.registry, "eventAPIs", store.New(eventAPIsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.channelNamespaces = store.Register(b.registry, "channelNamespaces", store.New(channelNamespacesKeyFn))
		b.channelNamespacesByAPI = b.channelNamespaces.AddIndex("byAPI", channelNamespaceAPIIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.sourceAssocs = store.Register(b.registry, "sourceAssocs", store.New(sourceAssocsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.introspections = store.Register(b.registry, "introspections", store.New(introspectionsKeyFn))
	},
}
