package dms

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T (region -> identifier -> value) resource field on
// InMemoryBackend is replaced with a single *store.Table[T], composite-keyed
// by region+identifier so that same-named resources stay isolated across
// regions exactly as the old per-region nested map did. Secondary
// *store.Index[T] values replace the old *ByARN / *ByID sibling maps -- and
// the ad hoc "scan every value in the region for a matching ARN" fallback
// loops several Delete/Modify methods used even where a ByARN map already
// existed (e.g. DeleteMigrationProject, ModifyReplicationConfig). See
// pkgs/store's package doc and the services/ec2 (commit 12e611a4,
// data-driven registration slice) and services/ses (commit e9af4cc7,
// identity-field-for-index-derivation) pilots for the pattern this follows.
//
// Every ARN/ID secondary index key -- and the connection/assessment-run/
// fleet-advisor-database primary key -- is itself region-prefixed via
// [regionKey], reproducing the old code's behavior of scoping every lookup
// (including ARN lookups, even though an ARN already embeds its region and
// is therefore globally unique on its own) to the request region's nested
// map. This is redundant given DMS resources never span regions (see
// getRegion's doc comment), but preserves the old lookup semantics
// byte-for-byte rather than silently widening what an ARN lookup can find.
//
// Four value types (Connection, AssessmentRun, FleetAdvisorDatabase,
// MetadataModelRequest) gained a new Region field so their store.Table keyFn
// -- a pure function of the value's own fields -- has a region to key on;
// none of the four carried one before because their old nested map already
// supplied region via the outer map key. This is purely additive to the
// exported API (a new field on an already-exported struct).
//
// Every table also gets a "byRegion" (or, for metadataModelRequests,
// "byProject") index: [store.Table.All]/[store.Table.Range] have no way to
// filter by key, only by value, so a region-scoped "list everything" -- what
// nearly every Describe* method needs -- stays an O(region size) index
// lookup instead of an O(all regions) scan, matching the old per-region
// nested map's performance characteristic.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// regionKey builds a composite store.Table/store.Index key "<region>|<rest>"
// used by every per-region resource table and its secondary indexes,
// mirroring the outer(region)/inner(identifier or ARN) nesting of the maps
// it replaces.
func regionKey(region, id string) string { return region + "|" + id }

// lookupUnique returns the single value grouped under key in idx, or
// (nil, false) if zero values are grouped there. Every idx this is called
// with is keyed by a field that is unique within its region (an ARN or a
// generated ID), so more than one grouped entry never legitimately occurs --
// the same guarantee a plain map[string]*T index (what these idx values
// replace) gave by construction.
func lookupUnique[V any](idx *store.Index[V], key string) (*V, bool) {
	group := idx.Get(key)
	if len(group) != 1 {
		return nil, false
	}

	return group[0], true
}

// describeByIdentifierOrARN implements the "try identifier, then ARN index,
// then list everything in the region" shape shared by every plain
// DescribeXxx(ctx, identifierOrArn) method (DescribeReplicationInstances,
// DescribeEndpoints, DescribeReplicationTasks, ...). Factoring it out here
// keeps those methods from being byte-for-byte duplicates of each other.
func describeByIdentifierOrARN[V any](
	t *store.Table[V],
	byARN, byRegion *store.Index[V],
	region, identifierOrArn string,
) []*V {
	if identifierOrArn != "" {
		if v, ok := t.Get(regionKey(region, identifierOrArn)); ok {
			cp := *v

			return []*V{&cp}
		}

		if v, ok := lookupUnique(byARN, regionKey(region, identifierOrArn)); ok {
			cp := *v

			return []*V{&cp}
		}

		return []*V{}
	}

	items := byRegion.Get(region)
	list := make([]*V, 0, len(items))

	for _, v := range items {
		cp := *v
		list = append(list, &cp)
	}

	return list
}

func replicationInstanceKeyFn(v *ReplicationInstance) string {
	return regionKey(v.Region, v.ReplicationInstanceIdentifier)
}

func endpointKeyFn(v *Endpoint) string { return regionKey(v.Region, v.EndpointIdentifier) }

func replicationTaskKeyFn(v *ReplicationTask) string {
	return regionKey(v.Region, v.ReplicationTaskIdentifier)
}

func dataMigrationKeyFn(v *DataMigration) string { return regionKey(v.Region, v.DataMigrationName) }

func dataProviderKeyFn(v *DataProvider) string { return regionKey(v.Region, v.DataProviderName) }

func eventSubscriptionKeyFn(v *EventSubscription) string {
	return regionKey(v.Region, v.SubscriptionName)
}

func fleetAdvisorCollectorKeyFn(v *FleetAdvisorCollector) string {
	return regionKey(v.Region, v.CollectorName)
}

func instanceProfileKeyFn(v *InstanceProfile) string {
	return regionKey(v.Region, v.InstanceProfileName)
}

func certificateKeyFn(v *Certificate) string { return regionKey(v.Region, v.CertificateIdentifier) }

func replicationSubnetGroupKeyFn(v *ReplicationSubnetGroup) string {
	return regionKey(v.Region, v.ReplicationSubnetGroupIdentifier)
}

func migrationProjectKeyFn(v *MigrationProject) string {
	return regionKey(v.Region, v.MigrationProjectName)
}

func replicationConfigKeyFn(v *ReplicationConfig) string {
	return regionKey(v.Region, v.ReplicationConfigIdentifier)
}

// connectionKeyFn reproduces the old "<replicationInstanceArn>:<endpointArn>"
// composite inner key, region-prefixed like every other table here.
func connectionKeyFn(v *Connection) string {
	return regionKey(v.Region, v.ReplicationInstanceArn+":"+v.EndpointArn)
}

// assessmentRunKeyFn keys by ARN (the old store's inner key was the ARN
// itself, not a separate identifier), region-prefixed.
func assessmentRunKeyFn(v *AssessmentRun) string {
	return regionKey(v.Region, v.ReplicationTaskAssessmentRunArn)
}

func fleetAdvisorDatabaseKeyFn(v *FleetAdvisorDatabase) string {
	return regionKey(v.Region, v.DatabaseID)
}

// metadataModelRequestProjectKey is the composite region+project key shared
// by metadataModelRequestKeyFn and the "byProject" index -- it mirrors the
// old two-level region -> projectARN nesting.
func metadataModelRequestProjectKey(region, projectARN string) string {
	return regionKey(region, projectARN)
}

// metadataModelRequestKey builds the full three-level composite key
// (region, projectARN, reqID), reproducing the old triple-nested map.
func metadataModelRequestKey(region, projectARN, reqID string) string {
	return metadataModelRequestProjectKey(region, projectARN) + "|" + reqID
}

func metadataModelRequestKeyFn(v *MetadataModelRequest) string {
	return metadataModelRequestKey(v.Region, v.MigrationProjectIdentifier, v.RequestIdentifier)
}

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. It must be called during construction
// only, never on every Reset(): store.Register panics on a duplicate name,
// so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps because their values are not *T (or not pointer-typed at
// all): tasksByInstanceARN and tasksByEndpointARN are reverse-index sets
// (map[string]map[string]struct{}), events and recommendations are
// region-keyed slices, and endpointSchemas is region -> endpointARN ->
// []string.
func registerAllTables(b *InMemoryBackend) {
	registerCoreReplicationTables(b)
	registerDataAndProfileTables(b)
	registerProjectAndMiscTables(b)
}

// registerCoreReplicationTables registers the replication-instance/endpoint/
// task collections -- the primary resources a DMS replication path is built
// from. Split out of registerAllTables purely to keep that function under
// the project's function-length lint threshold; the three groups have no
// other significance.
func registerCoreReplicationTables(b *InMemoryBackend) {
	b.replicationInstances = store.Register(b.registry, "replicationInstances", store.New(replicationInstanceKeyFn))
	b.replicationInstancesByARN = b.replicationInstances.AddIndex(
		"byARN", func(v *ReplicationInstance) string { return regionKey(v.Region, v.ReplicationInstanceArn) },
	)
	b.replicationInstancesByRegion = b.replicationInstances.AddIndex(
		"byRegion", func(v *ReplicationInstance) string { return v.Region },
	)

	b.endpoints = store.Register(b.registry, "endpoints", store.New(endpointKeyFn))
	b.endpointsByARN = b.endpoints.AddIndex(
		"byARN", func(v *Endpoint) string { return regionKey(v.Region, v.EndpointArn) },
	)
	b.endpointsByRegion = b.endpoints.AddIndex("byRegion", func(v *Endpoint) string { return v.Region })

	b.replicationTasks = store.Register(b.registry, "replicationTasks", store.New(replicationTaskKeyFn))
	b.replicationTasksByARN = b.replicationTasks.AddIndex(
		"byARN", func(v *ReplicationTask) string { return regionKey(v.Region, v.ReplicationTaskArn) },
	)
	b.replicationTasksByRegion = b.replicationTasks.AddIndex(
		"byRegion", func(v *ReplicationTask) string { return v.Region },
	)
}

// registerDataAndProfileTables registers the data-migration/data-provider/
// event-subscription/Fleet-Advisor-collector/instance-profile/certificate
// collections. See registerCoreReplicationTables for why this is split out.
func registerDataAndProfileTables(b *InMemoryBackend) {
	b.dataMigrations = store.Register(b.registry, "dataMigrations", store.New(dataMigrationKeyFn))
	b.dataMigrationsByARN = b.dataMigrations.AddIndex(
		"byARN", func(v *DataMigration) string { return regionKey(v.Region, v.DataMigrationArn) },
	)
	b.dataMigrationsByRegion = b.dataMigrations.AddIndex(
		"byRegion", func(v *DataMigration) string { return v.Region },
	)

	b.dataProviders = store.Register(b.registry, "dataProviders", store.New(dataProviderKeyFn))
	b.dataProvidersByARN = b.dataProviders.AddIndex(
		"byARN", func(v *DataProvider) string { return regionKey(v.Region, v.DataProviderArn) },
	)
	b.dataProvidersByRegion = b.dataProviders.AddIndex(
		"byRegion", func(v *DataProvider) string { return v.Region },
	)

	b.eventSubscriptions = store.Register(b.registry, "eventSubscriptions", store.New(eventSubscriptionKeyFn))
	b.eventSubscriptionsByRegion = b.eventSubscriptions.AddIndex(
		"byRegion", func(v *EventSubscription) string { return v.Region },
	)

	b.fleetAdvisorCollectors = store.Register(
		b.registry, "fleetAdvisorCollectors", store.New(fleetAdvisorCollectorKeyFn),
	)
	b.fleetAdvisorCollectorsByID = b.fleetAdvisorCollectors.AddIndex(
		"byID", func(v *FleetAdvisorCollector) string { return regionKey(v.Region, v.CollectorReferencedID) },
	)
	b.fleetAdvisorCollectorsByRegion = b.fleetAdvisorCollectors.AddIndex(
		"byRegion", func(v *FleetAdvisorCollector) string { return v.Region },
	)

	b.instanceProfiles = store.Register(b.registry, "instanceProfiles", store.New(instanceProfileKeyFn))
	b.instanceProfilesByARN = b.instanceProfiles.AddIndex(
		"byARN", func(v *InstanceProfile) string { return regionKey(v.Region, v.InstanceProfileArn) },
	)
	b.instanceProfilesByRegion = b.instanceProfiles.AddIndex(
		"byRegion", func(v *InstanceProfile) string { return v.Region },
	)

	b.certificates = store.Register(b.registry, "certificates", store.New(certificateKeyFn))
	b.certificatesByARN = b.certificates.AddIndex(
		"byARN", func(v *Certificate) string { return regionKey(v.Region, v.CertificateArn) },
	)
	b.certificatesByRegion = b.certificates.AddIndex("byRegion", func(v *Certificate) string { return v.Region })
}

// registerProjectAndMiscTables registers the migration-project/subnet-group/
// replication-config collections plus the smaller connection/assessment-run/
// Fleet-Advisor-database/metadata-model-request tables. See
// registerCoreReplicationTables for why this is split out.
func registerProjectAndMiscTables(b *InMemoryBackend) {
	b.replicationSubnetGroups = store.Register(
		b.registry, "replicationSubnetGroups", store.New(replicationSubnetGroupKeyFn),
	)
	b.replicationSubnetGroupsByARN = b.replicationSubnetGroups.AddIndex(
		"byARN", func(v *ReplicationSubnetGroup) string { return regionKey(v.Region, v.ReplicationSubnetGroupArn) },
	)
	b.replicationSubnetGroupsByRegion = b.replicationSubnetGroups.AddIndex(
		"byRegion", func(v *ReplicationSubnetGroup) string { return v.Region },
	)

	b.migrationProjects = store.Register(b.registry, "migrationProjects", store.New(migrationProjectKeyFn))
	b.migrationProjectsByARN = b.migrationProjects.AddIndex(
		"byARN", func(v *MigrationProject) string { return regionKey(v.Region, v.MigrationProjectArn) },
	)
	b.migrationProjectsByRegion = b.migrationProjects.AddIndex(
		"byRegion", func(v *MigrationProject) string { return v.Region },
	)

	b.replicationConfigs = store.Register(b.registry, "replicationConfigs", store.New(replicationConfigKeyFn))
	b.replicationConfigsByARN = b.replicationConfigs.AddIndex(
		"byARN", func(v *ReplicationConfig) string { return regionKey(v.Region, v.ReplicationConfigArn) },
	)
	b.replicationConfigsByRegion = b.replicationConfigs.AddIndex(
		"byRegion", func(v *ReplicationConfig) string { return v.Region },
	)

	b.connections = store.Register(b.registry, "connections", store.New(connectionKeyFn))
	b.connectionsByRegion = b.connections.AddIndex("byRegion", func(v *Connection) string { return v.Region })

	b.assessmentRuns = store.Register(b.registry, "assessmentRuns", store.New(assessmentRunKeyFn))
	b.assessmentRunsByRegion = b.assessmentRuns.AddIndex(
		"byRegion", func(v *AssessmentRun) string { return v.Region },
	)

	b.fleetAdvisorDatabases = store.Register(b.registry, "fleetAdvisorDatabases", store.New(fleetAdvisorDatabaseKeyFn))
	b.fleetAdvisorDatabasesByRegion = b.fleetAdvisorDatabases.AddIndex(
		"byRegion", func(v *FleetAdvisorDatabase) string { return v.Region },
	)

	b.metadataModelRequests = store.Register(b.registry, "metadataModelRequests", store.New(metadataModelRequestKeyFn))
	b.metadataModelRequestsByProject = b.metadataModelRequests.AddIndex(
		"byProject",
		func(v *MetadataModelRequest) string {
			return metadataModelRequestProjectKey(v.Region, v.MigrationProjectIdentifier)
		},
	)
}
