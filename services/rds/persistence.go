package rds

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Instances                 map[string]*DBInstance                        `json:"instances"`
	Snapshots                 map[string]*DBSnapshot                        `json:"snapshots"`
	SubnetGroups              map[string]*DBSubnetGroup                     `json:"subnetGroups"`
	Tags                      map[string][]Tag                              `json:"tags"`
	ParameterGroups           map[string]*DBParameterGroup                  `json:"parameterGroups"`
	ClusterParameterGroups    map[string]*DBParameterGroup                  `json:"clusterParameterGroups"`
	OptionGroups              map[string]*OptionGroup                       `json:"optionGroups"`
	Clusters                  map[string]*DBCluster                         `json:"clusters"`
	ClusterSnapshots          map[string]*DBClusterSnapshot                 `json:"clusterSnapshots"`
	ClusterEndpoints          map[string]*DBClusterEndpoint                 `json:"clusterEndpoints"`
	ExportTasks               map[string]*ExportTask                        `json:"exportTasks"`
	GlobalClusters            map[string]*GlobalCluster                     `json:"globalClusters"`
	ClusterRoles              map[string][]string                           `json:"clusterRoles"`
	InstanceRoles             map[string][]string                           `json:"instanceRoles"`
	EventSubscriptions        map[string]*EventSubscription                 `json:"eventSubscriptions"`
	DBSecurityGroups          map[string]*DBSecurityGroup                   `json:"dbSecurityGroups"`
	BlueGreenDeployments      map[string]*BlueGreenDeployment               `json:"blueGreenDeployments"`
	SnapshotAttributes        map[string]*DBSnapshotAttributesResult        `json:"snapshotAttributes"`
	ClusterSnapshotAttributes map[string]*DBClusterSnapshotAttributesResult `json:"clusterSnapshotAttributes"`
	ReservedInstances         map[string]*ReservedDBInstance                `json:"reservedInstances"`
	Recommendations           map[string]*DBRecommendation                  `json:"recommendations"`
	Proxies                   map[string]*DBProxy                           `json:"proxies"`
	ProxyTargetGroups         map[string]*DBProxyTargetGroup                `json:"proxyTargetGroups"`
	ProxyTargets              map[string][]DBProxyTarget                    `json:"proxyTargets"`
	ProxyEndpoints            map[string]*DBProxyEndpoint                   `json:"proxyEndpoints"`
	InstanceReadyAt           map[string]time.Time                          `json:"instanceReadyAt"`
	ClusterReadyAt            map[string]time.Time                          `json:"clusterReadyAt"`
	CustomEngineVersions      map[string]*CustomDBEngineVersion             `json:"customEngineVersions"`
	AutomatedBackups          map[string]*DBInstanceAutomatedBackup         `json:"automatedBackups"`
	ShardGroups               map[string]*DBShardGroup                      `json:"shardGroups"`
	Integrations              map[string]*Integration                       `json:"integrations"`
	TenantDatabases           map[string]*TenantDatabase                    `json:"tenantDatabases"`
	ClusterAutomatedBackups   map[string]*DBClusterAutomatedBackup          `json:"clusterAutomatedBackups"`
	SnapshotTenantDatabases   map[string][]*DBSnapshotTenantDatabase        `json:"snapshotTenantDatabases"`
	AccountID                 string                                        `json:"accountID"`
	Region                    string                                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:                 b.instances,
		Snapshots:                 b.snapshots,
		SubnetGroups:              b.subnetGroups,
		Tags:                      b.tags,
		ParameterGroups:           b.parameterGroups,
		ClusterParameterGroups:    b.clusterParameterGroups,
		OptionGroups:              b.optionGroups,
		Clusters:                  b.clusters,
		ClusterSnapshots:          b.clusterSnapshots,
		ClusterEndpoints:          b.clusterEndpoints,
		ExportTasks:               b.exportTasks,
		GlobalClusters:            b.globalClusters,
		ClusterRoles:              b.clusterRoles,
		InstanceRoles:             b.instanceRoles,
		EventSubscriptions:        b.eventSubscriptions,
		DBSecurityGroups:          b.dbSecurityGroups,
		BlueGreenDeployments:      b.blueGreenDeployments,
		SnapshotAttributes:        b.snapshotAttributes,
		ClusterSnapshotAttributes: b.clusterSnapshotAttributes,
		ReservedInstances:         b.reservedInstances,
		Recommendations:           b.recommendations,
		Proxies:                   b.proxies,
		ProxyTargetGroups:         b.proxyTargetGroups,
		ProxyTargets:              b.proxyTargets,
		ProxyEndpoints:            b.proxyEndpoints,
		InstanceReadyAt:           b.instanceReadyAt,
		ClusterReadyAt:            b.clusterReadyAt,
		AccountID:                 b.accountID,
		Region:                    b.region,
		CustomEngineVersions:      b.customEngineVersions,
		AutomatedBackups:          b.automatedBackups,
		ShardGroups:               b.shardGroups,
		Integrations:              b.integrations,
		TenantDatabases:           b.tenantDatabases,
		ClusterAutomatedBackups:   b.clusterAutomatedBackups,
		SnapshotTenantDatabases:   b.snapshotTenantDatabases,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "rds: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "rds", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.instances = snap.Instances
	b.snapshots = snap.Snapshots
	b.subnetGroups = snap.SubnetGroups
	b.tags = snap.Tags
	b.parameterGroups = snap.ParameterGroups
	b.clusterParameterGroups = snap.ClusterParameterGroups
	b.optionGroups = snap.OptionGroups
	b.clusters = snap.Clusters
	b.clusterSnapshots = snap.ClusterSnapshots
	b.clusterEndpoints = snap.ClusterEndpoints
	b.exportTasks = snap.ExportTasks
	b.globalClusters = snap.GlobalClusters
	b.clusterRoles = snap.ClusterRoles
	b.instanceRoles = snap.InstanceRoles
	b.eventSubscriptions = snap.EventSubscriptions
	b.dbSecurityGroups = snap.DBSecurityGroups
	b.blueGreenDeployments = snap.BlueGreenDeployments
	b.snapshotAttributes = snap.SnapshotAttributes
	b.clusterSnapshotAttributes = snap.ClusterSnapshotAttributes
	b.reservedInstances = snap.ReservedInstances
	b.recommendations = snap.Recommendations
	b.proxies = snap.Proxies
	b.proxyTargetGroups = snap.ProxyTargetGroups
	b.proxyTargets = snap.ProxyTargets
	b.proxyEndpoints = snap.ProxyEndpoints
	b.instanceReadyAt = snap.InstanceReadyAt
	if snap.ClusterReadyAt != nil {
		b.clusterReadyAt = snap.ClusterReadyAt
	} else {
		b.clusterReadyAt = make(map[string]time.Time)
	}
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.customEngineVersions = snap.CustomEngineVersions
	b.automatedBackups = snap.AutomatedBackups
	b.shardGroups = snap.ShardGroups
	b.integrations = snap.Integrations
	b.tenantDatabases = snap.TenantDatabases
	b.clusterAutomatedBackups = snap.ClusterAutomatedBackups
	b.snapshotTenantDatabases = snap.SnapshotTenantDatabases
	// FIS fault state is transient — clear it on restore so stale faults are not retained.
	b.fisFailoverFaults = make(map[string]time.Time)

	return nil
}

// ensureNonNilMaps initialises any nil maps in a deserialized snapshot so the backend
// never operates on nil maps after a restore.
func ensureNonNilMaps(snap *backendSnapshot) {
	ensureNonNilCoreMaps(snap)
	ensureNonNilExtendedMaps(snap)
	ensureNonNilBatch1Maps(snap)
}

// ensureNonNilCoreMaps initialises the core resource maps (instances, snapshots, subnet groups, tags,
// parameter groups, option groups, clusters, cluster snapshots, cluster endpoints, export tasks, global clusters).
func ensureNonNilCoreMaps(snap *backendSnapshot) {
	if snap.Instances == nil {
		snap.Instances = make(map[string]*DBInstance)
	}

	if snap.Snapshots == nil {
		snap.Snapshots = make(map[string]*DBSnapshot)
	}

	if snap.SubnetGroups == nil {
		snap.SubnetGroups = make(map[string]*DBSubnetGroup)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string][]Tag)
	}

	if snap.ParameterGroups == nil {
		snap.ParameterGroups = make(map[string]*DBParameterGroup)
	}

	if snap.ClusterParameterGroups == nil {
		snap.ClusterParameterGroups = make(map[string]*DBParameterGroup)
	}

	if snap.OptionGroups == nil {
		snap.OptionGroups = make(map[string]*OptionGroup)
	}

	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*DBCluster)
	}

	if snap.ClusterSnapshots == nil {
		snap.ClusterSnapshots = make(map[string]*DBClusterSnapshot)
	}

	if snap.ClusterEndpoints == nil {
		snap.ClusterEndpoints = make(map[string]*DBClusterEndpoint)
	}

	if snap.ExportTasks == nil {
		snap.ExportTasks = make(map[string]*ExportTask)
	}

	if snap.GlobalClusters == nil {
		snap.GlobalClusters = make(map[string]*GlobalCluster)
	}

	if snap.InstanceReadyAt == nil {
		snap.InstanceReadyAt = make(map[string]time.Time)
	}
}

// ensureNonNilExtendedMaps initialises the extended resource maps added later
// (cluster/instance roles, event subscriptions, security groups, blue/green deployments,
// snapshot attributes, reserved instances, and recommendations).
func ensureNonNilExtendedMaps(snap *backendSnapshot) {
	if snap.ClusterRoles == nil {
		snap.ClusterRoles = make(map[string][]string)
	}

	if snap.InstanceRoles == nil {
		snap.InstanceRoles = make(map[string][]string)
	}

	if snap.EventSubscriptions == nil {
		snap.EventSubscriptions = make(map[string]*EventSubscription)
	}

	if snap.DBSecurityGroups == nil {
		snap.DBSecurityGroups = make(map[string]*DBSecurityGroup)
	}

	if snap.BlueGreenDeployments == nil {
		snap.BlueGreenDeployments = make(map[string]*BlueGreenDeployment)
	}

	if snap.SnapshotAttributes == nil {
		snap.SnapshotAttributes = make(map[string]*DBSnapshotAttributesResult)
	}

	if snap.ClusterSnapshotAttributes == nil {
		snap.ClusterSnapshotAttributes = make(map[string]*DBClusterSnapshotAttributesResult)
	}

	if snap.ReservedInstances == nil {
		snap.ReservedInstances = make(map[string]*ReservedDBInstance)
	}

	if snap.Recommendations == nil {
		snap.Recommendations = make(map[string]*DBRecommendation)
	}

	if snap.Proxies == nil {
		snap.Proxies = make(map[string]*DBProxy)
	}

	if snap.ProxyTargetGroups == nil {
		snap.ProxyTargetGroups = make(map[string]*DBProxyTargetGroup)
	}

	if snap.ProxyTargets == nil {
		snap.ProxyTargets = make(map[string][]DBProxyTarget)
	}

	if snap.ProxyEndpoints == nil {
		snap.ProxyEndpoints = make(map[string]*DBProxyEndpoint)
	}
}

// ensureNonNilBatch1Maps initialises the batch-1 resource maps added in the first
// stateful-ops batch (custom engine versions, shard groups, integrations, tenant
// databases, cluster/instance automated backups, and snapshot tenant databases).
func ensureNonNilBatch1Maps(snap *backendSnapshot) {
	if snap.CustomEngineVersions == nil {
		snap.CustomEngineVersions = make(map[string]*CustomDBEngineVersion)
	}

	if snap.AutomatedBackups == nil {
		snap.AutomatedBackups = make(map[string]*DBInstanceAutomatedBackup)
	}

	if snap.ShardGroups == nil {
		snap.ShardGroups = make(map[string]*DBShardGroup)
	}

	if snap.Integrations == nil {
		snap.Integrations = make(map[string]*Integration)
	}

	if snap.TenantDatabases == nil {
		snap.TenantDatabases = make(map[string]*TenantDatabase)
	}

	if snap.ClusterAutomatedBackups == nil {
		snap.ClusterAutomatedBackups = make(map[string]*DBClusterAutomatedBackup)
	}

	if snap.SnapshotTenantDatabases == nil {
		snap.SnapshotTenantDatabases = make(map[string][]*DBSnapshotTenantDatabase)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.defaultRegion == "" || len(h.backends) == 0 {
		return h.Backend.Snapshot(ctx)
	}

	snaps := make(map[string]json.RawMessage)
	for region, b := range h.backends {
		snaps[region] = b.Snapshot(ctx)
	}
	data, _ := json.Marshal(snaps)

	return data
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.defaultRegion == "" || len(h.backends) == 0 {
		return h.Backend.Restore(ctx, data)
	}

	var snaps map[string]json.RawMessage
	if err := json.Unmarshal(data, &snaps); err != nil {
		// Fallback for backwards compatibility with single-region snapshots.
		return h.Backend.Restore(ctx, data)
	}

	// Check if this might be a single-region snapshot that happens to unmarshal as map[string]json.RawMessage.
	// Since backendSnapshot has "instances", "snapshots", etc. as keys, they might match.
	// We can check if "instances" exists as a key.
	if _, hasInstances := snaps["instances"]; hasInstances {
		return h.Backend.Restore(ctx, data)
	}

	for region, raw := range snaps {
		b, ok := h.backends[region]
		if !ok {
			b = NewInMemoryBackend(h.accountID, region)
			h.backends[region] = b
			h.handlers[region] = &Handler{Backend: b}
		}
		if err := b.Restore(ctx, raw); err != nil {
			return err
		}
	}

	return nil
}
