package redshift

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend whose key is a pure
// function of the stored value's own fields is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 conversion (commit 12e611a4) and services/sqs pilot
// (commit 0f09d77c) for the pattern this follows.
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps (see persistence.go for how they are still persisted,
// and store.go for their declarations):
//   - activeResizes: value type ResizeProgress carries no ClusterIdentifier
//     field of its own -- it is keyed externally by clusterID (see
//     AddActiveResizeInternal, which takes clusterID as a separate
//     parameter from the ResizeProgress value). Not a pure function of the
//     value's own fields, which store.Table requires.
//   - snapshotCopyConfigs: value type SnapshotCopyConfig carries no
//     ClusterIdentifier field of its own -- same external-keying issue.
//   - loggingStatuses: value type LoggingStatus carries no ClusterIdentifier
//     field of its own -- same external-keying issue.
//   - clusterTransitions: in-flight lifecycle state, intentionally never
//     persisted (see Restore) and also keyed externally by cluster ID.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func clustersKeyFn(v *Cluster) string { return v.ClusterIdentifier }

func reservedNodesKeyFn(v *ReservedNode) string { return v.ReservedNodeID }

func partnersKeyFn(v *Partner) string {
	return partnerKey(v.ClusterIdentifier, v.DatabaseName, v.PartnerName)
}

func dataSharesKeyFn(v *DataShare) string { return v.DataShareArn }

func securityGroupsKeyFn(v *ClusterSecurityGroup) string { return v.ClusterSecurityGroupName }

func snapshotsKeyFn(v *Snapshot) string { return v.SnapshotIdentifier }

func endpointAuthsKeyFn(v *EndpointAuthorization) string {
	return endpointAuthKey(v.ClusterIdentifier, v.Grantee)
}

func parameterGroupsKeyFn(v *ClusterParameterGroup) string { return v.ParameterGroupName }

func subnetGroupsKeyFn(v *ClusterSubnetGroup) string { return v.ClusterSubnetGroupName }

func eventSubscriptionsKeyFn(v *EventSubscription) string { return v.CustSubscriptionID }

func eventsKeyFn(v *Event) string { return v.EventID }

func snapshotCopyGrantsKeyFn(v *SnapshotCopyGrant) string { return v.SnapshotCopyGrantName }

func snapshotSchedulesKeyFn(v *SnapshotSchedule) string { return v.ScheduleIdentifier }

func usageLimitsKeyFn(v *UsageLimit) string { return v.UsageLimitID }

func authProfilesKeyFn(v *AuthenticationProfile) string { return v.AuthenticationProfileName }

func resourcePoliciesKeyFn(v *ResourcePolicy) string { return v.ResourceArn }

func tableRestoresKeyFn(v *TableRestoreStatus) string { return v.TableRestoreRequestID }

func hsmClientCertsKeyFn(v *HsmClientCertificate) string { return v.HsmClientCertificateIdentifier }

func hsmConfigsKeyFn(v *HsmConfiguration) string { return v.HsmConfigurationIdentifier }

func scheduledActionsKeyFn(v *ScheduledAction) string { return v.ScheduledActionName }

func customDomainsKeyFn(v *CustomDomainAssociation) string {
	return v.ClusterIdentifier + ":" + v.CustomDomainName
}

func endpointAccessesKeyFn(v *EndpointAccess) string { return v.EndpointName }

func integrationsKeyFn(v *Integration) string { return v.IntegrationName }

func idcApplicationsKeyFn(v *IdcApplication) string { return v.IdcApplicationName }

func qev2IdcApplicationsKeyFn(v *Qev2IdcApplication) string { return v.Qev2IdcApplicationName }

func slNamespacesKeyFn(v *Namespace) string { return v.NamespaceName }

func slWorkgroupsKeyFn(v *Workgroup) string { return v.WorkgroupName }

func slSnapshotsKeyFn(v *ServerlessSnapshot) string { return v.SnapshotName }

func slUsageLimitsKeyFn(v *ServerlessUsageLimit) string { return v.UsageLimitID }

func slScheduledActionsKeyFn(v *ServerlessScheduledAction) string { return v.ScheduledActionName }

func slResourceTagsKeyFn(v *slResourceTagSet) string { return v.ResourceArn }

func slCustomDomainsSLKeyFn(v *ServerlessCustomDomainAssociation) string { return v.CustomDomainName }

func slResourcePoliciesKeyFn(v *ServerlessResourcePolicy) string { return v.ResourceArn }

func slSnapshotCopyConfigKeyFn(v *ServerlessSnapshotCopyConfiguration) string {
	return v.SnapshotCopyConfigurationID
}

func slRecoveryPointsKeyFn(v *RecoveryPoint) string { return v.RecoveryPointID }

func slTableRestoreStatusesKeyFn(v *ServerlessTableRestoreStatus) string {
	return v.TableRestoreRequestID
}

func slEndpointAccessesKeyFn(v *ServerlessEndpointAccess) string { return v.EndpointName }

func slLakehouseConfigKeyFn(v *ServerlessLakehouseConfig) string { return v.NamespaceName }

func namespaceRegistrationsKeyFn(v *NamespaceRegistration) string { return v.NamespaceKey }

func clusterLakehouseConfigKeyFn(v *ClusterLakehouseConfig) string { return v.ClusterIdentifier }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately
// after b.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// registry.ResetAll() instead (see InMemoryBackend.Reset in store.go).
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
		b.clusters = store.Register(b.registry, "clusters", store.New(clustersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.reservedNodes = store.Register(b.registry, "reservedNodes", store.New(reservedNodesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.partners = store.Register(b.registry, "partners", store.New(partnersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dataShares = store.Register(b.registry, "dataShares", store.New(dataSharesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.securityGroups = store.Register(b.registry, "securityGroups", store.New(securityGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.snapshots = store.Register(b.registry, "snapshots", store.New(snapshotsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.endpointAuths = store.Register(b.registry, "endpointAuths", store.New(endpointAuthsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.parameterGroups = store.Register(b.registry, "parameterGroups", store.New(parameterGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.subnetGroups = store.Register(b.registry, "subnetGroups", store.New(subnetGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.eventSubscriptions = store.Register(b.registry, "eventSubscriptions", store.New(eventSubscriptionsKeyFn))
	},
	func(b *InMemoryBackend) { b.events = store.Register(b.registry, "events", store.New(eventsKeyFn)) },
	func(b *InMemoryBackend) {
		b.snapshotCopyGrants = store.Register(b.registry, "snapshotCopyGrants", store.New(snapshotCopyGrantsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.snapshotSchedules = store.Register(b.registry, "snapshotSchedules", store.New(snapshotSchedulesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.usageLimits = store.Register(b.registry, "usageLimits", store.New(usageLimitsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.authProfiles = store.Register(b.registry, "authProfiles", store.New(authProfilesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePoliciesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.tableRestores = store.Register(b.registry, "tableRestores", store.New(tableRestoresKeyFn))
	},
	func(b *InMemoryBackend) {
		b.hsmClientCerts = store.Register(b.registry, "hsmClientCerts", store.New(hsmClientCertsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.hsmConfigs = store.Register(b.registry, "hsmConfigs", store.New(hsmConfigsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.scheduledActions = store.Register(b.registry, "scheduledActions", store.New(scheduledActionsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.customDomains = store.Register(b.registry, "customDomains", store.New(customDomainsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.endpointAccesses = store.Register(b.registry, "endpointAccesses", store.New(endpointAccessesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.integrations = store.Register(b.registry, "integrations", store.New(integrationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.idcApplications = store.Register(b.registry, "idcApplications", store.New(idcApplicationsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.qev2IdcApplications = store.Register(
			b.registry, "qev2IdcApplications", store.New(qev2IdcApplicationsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.slNamespaces = store.Register(b.registry, "slNamespaces", store.New(slNamespacesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slWorkgroups = store.Register(b.registry, "slWorkgroups", store.New(slWorkgroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slSnapshots = store.Register(b.registry, "slSnapshots", store.New(slSnapshotsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slUsageLimits = store.Register(b.registry, "slUsageLimits", store.New(slUsageLimitsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slScheduledActions = store.Register(b.registry, "slScheduledActions", store.New(slScheduledActionsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slResourceTags = store.Register(b.registry, "slResourceTags", store.New(slResourceTagsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slCustomDomains = store.Register(b.registry, "slCustomDomainsSL", store.New(slCustomDomainsSLKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slResourcePolicies = store.Register(b.registry, "slResourcePolicies", store.New(slResourcePoliciesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slSnapshotCopyConfig = store.Register(
			b.registry, "slSnapshotCopyConfig", store.New(slSnapshotCopyConfigKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.slRecoveryPoints = store.Register(b.registry, "slRecoveryPoints", store.New(slRecoveryPointsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slTableRestoreStatuses = store.Register(
			b.registry, "slTableRestoreStatuses", store.New(slTableRestoreStatusesKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.slEndpointAccesses = store.Register(b.registry, "slEndpointAccesses", store.New(slEndpointAccessesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.slLakehouseConfig = store.Register(b.registry, "slLakehouseConfig", store.New(slLakehouseConfigKeyFn))
	},
	func(b *InMemoryBackend) {
		b.namespaceRegistrations = store.Register(
			b.registry, "namespaceRegistrations", store.New(namespaceRegistrationsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.clusterLakehouseConfig = store.Register(
			b.registry, "clusterLakehouseConfig", store.New(clusterLakehouseConfigKeyFn),
		)
	},
}

// tableKeys returns the key (per keyFn) of every value currently in t, in
// unspecified order. Used by rebuildServerlessIndexes to seed the sorted
// secondary indexes from a store.Table instead of a bare map.
func tableKeys[V any](t *store.Table[V], keyFn func(*V) string) []string {
	all := t.All()
	keys := make([]string, 0, len(all))

	for _, v := range all {
		keys = append(keys, keyFn(v))
	}

	return keys
}
