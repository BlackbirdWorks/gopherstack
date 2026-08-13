package rds

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c)
// conversions this follows.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment block above registerAllTables for the list and why.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// instancesKeyFn, snapshotsKeyFn, parameterGroupsKeyFn, clustersKeyFn, and
// clusterSnapshotsKeyFn normalize through normalizeID (see its doc comment):
// these five identifier families are case-insensitive on real AWS, while the
// value's own identifier field keeps the caller's original casing for wire
// output. parameterGroupsKeyFn backs both the parameterGroups AND
// clusterParameterGroups tables (both store *DBParameterGroup — AWS's
// DBClusterParameterGroupName is a distinct API name for the same shape).
func instancesKeyFn(v *DBInstance) string             { return normalizeID(v.DBInstanceIdentifier) }
func snapshotsKeyFn(v *DBSnapshot) string             { return normalizeID(v.DBSnapshotIdentifier) }
func subnetGroupsKeyFn(v *DBSubnetGroup) string       { return v.DBSubnetGroupName }
func parameterGroupsKeyFn(v *DBParameterGroup) string { return normalizeID(v.DBParameterGroupName) }
func optionGroupsKeyFn(v *OptionGroup) string         { return v.OptionGroupName }
func clustersKeyFn(v *DBCluster) string               { return normalizeID(v.DBClusterIdentifier) }
func clusterSnapshotsKeyFn(v *DBClusterSnapshot) string {
	return normalizeID(v.DBClusterSnapshotIdentifier)
}
func clusterEndpointsKeyFn(v *DBClusterEndpoint) string   { return v.DBClusterEndpointIdentifier }
func exportTasksKeyFn(v *ExportTask) string               { return v.ExportTaskIdentifier }
func globalClustersKeyFn(v *GlobalCluster) string         { return v.GlobalClusterIdentifier }
func eventSubscriptionsKeyFn(v *EventSubscription) string { return v.SubscriptionName }
func dbSecurityGroupsKeyFn(v *DBSecurityGroup) string     { return v.DBSecurityGroupName }
func blueGreenDeploymentsKeyFn(v *BlueGreenDeployment) string {
	return v.BlueGreenDeploymentIdentifier
}

// snapshotAttributesKeyFn / clusterSnapshotAttributesKeyFn also normalize:
// these tables are satellite data keyed by the same case-insensitive
// DBSnapshotIdentifier / DBClusterSnapshotIdentifier as the snapshots /
// clusterSnapshots tables above, so leaving them case-sensitive would
// re-introduce the same collision bug one level removed (e.g. Modify'ing
// attributes as "MySnap" then Describe'ing them as "mysnap" would wrongly
// report no attributes).
func snapshotAttributesKeyFn(v *DBSnapshotAttributesResult) string {
	return normalizeID(v.DBSnapshotIdentifier)
}
func clusterSnapshotAttributesKeyFn(v *DBClusterSnapshotAttributesResult) string {
	return normalizeID(v.DBClusterSnapshotIdentifier)
}
func reservedInstancesKeyFn(v *ReservedDBInstance) string { return v.ReservedDBInstanceID }
func recommendationsKeyFn(v *DBRecommendation) string     { return v.RecommendationID }
func proxiesKeyFn(v *DBProxy) string                      { return v.DBProxyName }
func proxyTargetGroupsKeyFn(v *DBProxyTargetGroup) string {
	return v.DBProxyName + "/" + v.TargetGroupName
}
func proxyEndpointsKeyFn(v *DBProxyEndpoint) string { return v.DBProxyEndpointName }
func customEngineVersionsKeyFn(v *CustomDBEngineVersion) string {
	return v.Engine + ":" + v.EngineVersion
}
func shardGroupsKeyFn(v *DBShardGroup) string { return v.DBShardGroupIdentifier }
func integrationsKeyFn(v *Integration) string { return v.IntegrationName }
func tenantDatabasesKeyFn(v *TenantDatabase) string {
	return tenantKey(v.DBInstanceIdentifier, v.TenantDBName)
}
func clusterAutomatedBackupsKeyFn(v *DBClusterAutomatedBackup) string { return v.DBClusterIdentifier }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because they are either slice-valued, transient scheduling
// state, or their map key is not a pure function of the stored value's own
// fields (store.Table requires the latter):
//   - tags: map[string][]Tag keyed by ARN, slice-valued
//   - instanceReadyAt / clusterReadyAt: transient reconciler-scheduling
//     timestamps, not persisted resource state with an identity field
//   - clusterRoles: map[string][]string, slice-valued
//   - instanceRoles: map[string]map[string]string, keyed by FeatureName per instance
//   - proxyTargets: map[string][]DBProxyTarget, slice-valued
//   - fisFailoverFaults: map[string]time.Time, transient FIS fault-injection
//     state explicitly cleared (not restored) on Restore
//   - automatedBackups: mixed keying convention across call sites (plain
//     DBInstanceIdentifier in the normal create-with-retention flow vs
//     "repl-"+ARN in StartDBInstanceAutomatedBackupsReplication) -- pre-existing
//     quirk, not a pure function of value identity (same exclusion class as
//     EC2's addressTransfers)
//   - snapshotTenantDatabases: map[string][]*DBSnapshotTenantDatabase, slice-valued
//   - piMetrics: map[string]map[string][]PIDataPoint, nested map
//   - instanceLogFiles: map[string][]DBLogFile, slice-valued
//   - instanceLogContent: map[string]map[string]string, nested map
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type. A closure list (rather than one
// statement per field in a single function body) keeps registerAllTables small
// regardless of how many resource tables the backend grows to.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.instances = store.Register(b.registry, "instances", store.New(instancesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.snapshots = store.Register(b.registry, "snapshots", store.New(snapshotsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.subnetGroups = store.Register(b.registry, "subnetGroups", store.New(subnetGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.parameterGroups = store.Register(b.registry, "parameterGroups", store.New(parameterGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clusterParameterGroups = store.Register(
			b.registry,
			"clusterParameterGroups",
			store.New(parameterGroupsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.optionGroups = store.Register(b.registry, "optionGroups", store.New(optionGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clusters = store.Register(b.registry, "clusters", store.New(clustersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clusterSnapshots = store.Register(b.registry, "clusterSnapshots", store.New(clusterSnapshotsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clusterEndpoints = store.Register(b.registry, "clusterEndpoints", store.New(clusterEndpointsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.exportTasks = store.Register(b.registry, "exportTasks", store.New(exportTasksKeyFn))
	},
	func(b *InMemoryBackend) {
		b.globalClusters = store.Register(b.registry, "globalClusters", store.New(globalClustersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.eventSubscriptions = store.Register(
			b.registry,
			"eventSubscriptions",
			store.New(eventSubscriptionsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.dbSecurityGroups = store.Register(b.registry, "dbSecurityGroups", store.New(dbSecurityGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.blueGreenDeployments = store.Register(
			b.registry,
			"blueGreenDeployments",
			store.New(blueGreenDeploymentsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.snapshotAttributes = store.Register(
			b.registry,
			"snapshotAttributes",
			store.New(snapshotAttributesKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.clusterSnapshotAttributes = store.Register(
			b.registry,
			"clusterSnapshotAttributes",
			store.New(clusterSnapshotAttributesKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.reservedInstances = store.Register(b.registry, "reservedInstances", store.New(reservedInstancesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.recommendations = store.Register(b.registry, "recommendations", store.New(recommendationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.proxies = store.Register(b.registry, "proxies", store.New(proxiesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.proxyTargetGroups = store.Register(b.registry, "proxyTargetGroups", store.New(proxyTargetGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.proxyEndpoints = store.Register(b.registry, "proxyEndpoints", store.New(proxyEndpointsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.customEngineVersions = store.Register(
			b.registry,
			"customEngineVersions",
			store.New(customEngineVersionsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.shardGroups = store.Register(b.registry, "shardGroups", store.New(shardGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.integrations = store.Register(b.registry, "integrations", store.New(integrationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.tenantDatabases = store.Register(b.registry, "tenantDatabases", store.New(tenantDatabasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clusterAutomatedBackups = store.Register(
			b.registry,
			"clusterAutomatedBackups",
			store.New(clusterAutomatedBackupsKeyFn),
		)
	},
}
