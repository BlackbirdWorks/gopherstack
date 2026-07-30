package opensearch

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c) /
// services/apigateway (commit 6da0334e) conversions this follows.
//
// Two resource families (domainDataSources: domainName -> dataSourceName ->
// *DataSource, and domainIndexes: domainName -> indexName -> *DomainIndex)
// were previously nested per-domain maps. store.Table has no notion of
// nesting, so each becomes a single flat table keyed by a composite
// "<domainName>#<childID>" string, with a secondary [store.Index] grouping by
// domain name for the "all X of domain Y" lookups the nested maps used to
// answer directly. This requires the child value type to carry its parent's
// domain name as a field purely for identity/keying (never on the wire) --
// DataSource and DomainIndex gained a `DomainName string `json:"-"`` field for
// exactly this reason.
//
// Two more single-value-per-domain maps (dryRuns: domainName -> *DryRunStatus,
// autoTunes: "autotune:"+domainName -> *AutoTuneConfig) have the same problem
// in miniature: the value type carried no field of its own to key a Table by,
// so DryRunStatus and AutoTuneConfig also gained a `DomainName string
// `json:"-"`` field.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see registerAllTables's doc for the full list and why.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func domainKeyFn(v *Domain) string    { return v.Name }
func domainARNKeyFn(v *Domain) string { return v.ARN }

func inboundConnectionKeyFn(v *InboundConnection) string   { return v.ConnectionID }
func outboundConnectionKeyFn(v *OutboundConnection) string { return v.ConnectionID }

// dataSourceKey builds the composite "<domainName>#<name>" key shared by
// every data source nested under a domain. Access sites use this directly so
// the exact same key is used for Put/Get/Delete.
func dataSourceKey(domainName, name string) string { return domainName + "#" + name }
func dataSourceKeyFn(v *DataSource) string         { return dataSourceKey(v.DomainName, v.Name) }

func directQueryDataSourceKeyFn(v *DirectQueryDataSource) string { return v.Name }

func vpcEndpointKeyFn(v *VpcEndpoint) string { return v.VpcEndpointID }

func applicationKeyFn(v *Application) string     { return v.ID }
func applicationNameKeyFn(v *Application) string { return v.Name }

func packageKeyFn(v *Package) string { return v.PackageID }

func reservedInstanceKeyFn(v *ReservedInstance) string { return v.ReservedInstanceID }

// domainIndexKey builds the composite "<domainName>#<indexName>" key shared
// by every index nested under a domain.
func domainIndexKey(domainName, indexName string) string { return domainName + "#" + indexName }
func domainIndexKeyFn(v *DomainIndex) string {
	return domainIndexKey(v.DomainName, v.IndexName)
}

func dryRunKeyFn(v *DryRunStatus) string { return v.DomainName }

func autoTuneConfigKeyFn(v *AutoTuneConfig) string { return autoTuneKey(v.DomainName) }

// slCollectionKeyFn/slAccessPolicyKeyFn/... reuse the serverless*Key helpers
// (defined in serverless.go, next to the other serverless
// constructors) so the table key matches exactly what every access site
// already computes. Unlike the apigateway "dirty" families, every serverless
// value type already carries the fields (Name, Type, ID) its key is built
// from, so these are all "clean" tables -- no identity field needed.
func slCollectionKeyFn(v *ServerlessCollection) string { return serverlessCollectionKey(v.Name) }

func slAccessPolicyKeyFn(v *ServerlessAccessPolicy) string {
	return serverlessAccessPolicyKey(v.Type, v.Name)
}

func slSecurityConfigKeyFn(v *ServerlessSecurityConfig) string {
	return serverlessSecurityConfigKey(v.ID)
}

func slEncryptionPolicyKeyFn(v *ServerlessEncryptionPolicy) string {
	return serverlessEncryptionPolicyKey(v.Type, v.Name)
}

func slNetworkPolicyKeyFn(v *ServerlessNetworkPolicy) string {
	return serverlessNetworkPolicyKey(v.Type, v.Name)
}

// dataSourceAttachmentKeyFn, capabilityKeyFn, migrationKeyFn, and
// workspaceKeyFn are defined alongside their families
// (data_source_attachments.go, capabilities.go, migrations.go,
// workspaces.go) since each already needs its composite-key helper there;
// only the table/index registrations live here, matching the convention this
// file documents.

// registerAllTables sets up every converted resource collection exactly once,
// at construction time.
//
// Two groups, split by whether their store.Table can be snapshotted directly:
//
//   - "Clean" tables (domains, inboundConnections, outboundConnections,
//     directQueryDataSources, vpcEndpoints, applications, packages,
//     reservedInstances, slCollections, slAccessPolicies, slSecurityConfigs,
//     slEncryptionPolicies, slNetworkPolicies, dataSourceAttachments,
//     capabilities, migrations, workspaces) are registered on b.registry via
//     store.Register, so persistence.go's Snapshot/Restore can drive them
//     through b.registry.SnapshotAll()/RestoreAll() -- their value types
//     marshal to JSON with no information loss. (Package.VersionHistory
//     carries a pre-existing `json:"-"` tag of its own, unrelated to this
//     conversion, so it was already excluded from persistence before this
//     change and remains so -- not a regression, just an existing quirk this
//     conversion preserves byte-for-byte.)
//   - "Dirty" tables (dryRuns, autoTunes, domainDataSources, domainIndexes)
//     are built with store.New but deliberately NOT registered on b.registry.
//     Their key depends on a field (DomainName) tagged `json:"-"` on the live
//     type, so a direct json.Marshal/Unmarshal round trip through
//     Table.Snapshot/Restore would silently drop that field and corrupt the
//     table's key on restore. persistence.go instead builds a throwaway DTO
//     [store.Registry] purely to get a correctly-tagged JSON encoding (mirrors
//     the services/sqs pilot, commit 0f09d77c, and services/apigateway,
//     commit 6da0334e), then restores the live tables directly via
//     Table.Restore. Because they aren't registered on b.registry,
//     InMemoryBackend.Reset resets each of them with an explicit Table.Reset()
//     call alongside b.registry.ResetAll().
//
// The following fields are deliberately plain maps, not store.Table at all:
//   - vpcAuthorizations (domainName -> []AuthorizedPrincipal),
//     scheduledActions (domainName -> []*ScheduledAction), domainMaintenances
//     (domainName -> []*DomainMaintenance), and upgradeHistory
//     ("upgrade:"+domainName -> []*UpgradeHistory) are all slice-valued maps:
//     the resource collection for a given key is the *whole ordered slice*
//     (append-and-trim-to-cap, or replace-wholesale), not a keyed set of
//     individually addressable *T values, so they don't fit store.Table's
//     keyed-by-identity shape.
//   - packageAssociations (packageID -> domainName -> bool) and
//     domainPackages (domainName -> packageID -> bool) are boolean-set
//     indexes recording a many-to-many association, not map[string]*T
//     resource collections -- the value has no identity of its own to key a
//     Table by, the same class of exception as services/apigateway's
//     usageOverrides / services/ec2's instanceIMDSOptions.
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
		b.domains = store.Register(b.registry, "domains", store.New(domainKeyFn))
		b.domainsByARN = b.domains.AddIndex("byARN", domainARNKeyFn)
	},
	func(b *InMemoryBackend) {
		b.inboundConnections = store.Register(b.registry, "inboundConnections", store.New(inboundConnectionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.outboundConnections = store.Register(b.registry, "outboundConnections", store.New(outboundConnectionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.directQueryDataSources = store.Register(
			b.registry, "directQueryDataSources", store.New(directQueryDataSourceKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.vpcEndpoints = store.Register(b.registry, "vpcEndpoints", store.New(vpcEndpointKeyFn))
	},
	func(b *InMemoryBackend) {
		b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
		b.applicationsByName = b.applications.AddIndex("byName", applicationNameKeyFn)
	},
	func(b *InMemoryBackend) {
		b.packages = store.Register(b.registry, "packages", store.New(packageKeyFn))
	},
	func(b *InMemoryBackend) {
		b.reservedInstances = store.Register(b.registry, "reservedInstances", store.New(reservedInstanceKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slCollections = store.Register(b.registry, "slCollections", store.New(slCollectionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slAccessPolicies = store.Register(b.registry, "slAccessPolicies", store.New(slAccessPolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slSecurityConfigs = store.Register(b.registry, "slSecurityConfigs", store.New(slSecurityConfigKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slEncryptionPolicies = store.Register(b.registry, "slEncryptionPolicies", store.New(slEncryptionPolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slNetworkPolicies = store.Register(b.registry, "slNetworkPolicies", store.New(slNetworkPolicyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dataSourceAttachments = store.Register(
			b.registry, "dataSourceAttachments", store.New(dataSourceAttachmentKeyFn),
		)
		b.dataSourceAttachmentsByApp = b.dataSourceAttachments.AddIndex(
			"byApplication", func(v *DataSourceAttachment) string { return v.ApplicationID },
		)
	},
	func(b *InMemoryBackend) {
		b.capabilities = store.Register(b.registry, "capabilities", store.New(capabilityKeyFn))
	},
	func(b *InMemoryBackend) {
		b.migrations = store.Register(b.registry, "migrations", store.New(migrationKeyFn))
		b.migrationsByApp = b.migrations.AddIndex(
			"byApplication", func(v *Migration) string { return v.ApplicationID },
		)
	},
	func(b *InMemoryBackend) {
		b.workspaces = store.Register(b.registry, "workspaces", store.New(workspaceKeyFn))
		b.workspacesByApp = b.workspaces.AddIndex(
			"byApplication", func(v *Workspace) string { return v.ApplicationID },
		)
	},
	// "Dirty" tables: store.New only, NOT store.Register -- see
	// registerAllTables doc.
	func(b *InMemoryBackend) {
		b.dryRuns = store.New(dryRunKeyFn)
	},
	func(b *InMemoryBackend) {
		b.autoTunes = store.New(autoTuneConfigKeyFn)
	},
	func(b *InMemoryBackend) {
		b.domainDataSources = store.New(dataSourceKeyFn)
		b.domainDataSourcesByDomain = b.domainDataSources.AddIndex(
			"byDomain", func(v *DataSource) string { return v.DomainName },
		)
	},
	func(b *InMemoryBackend) {
		b.domainIndexes = store.New(domainIndexKeyFn)
		b.domainIndexesByDomain = b.domainIndexes.AddIndex(
			"byDomain", func(v *DomainIndex) string { return v.DomainName },
		)
	},
}
