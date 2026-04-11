package rds

import (
	"encoding/json"
	"time"
)

type backendSnapshot struct {
	Instances              map[string]*DBInstance          `json:"instances"`
	Snapshots              map[string]*DBSnapshot          `json:"snapshots"`
	SubnetGroups           map[string]*DBSubnetGroup       `json:"subnetGroups"`
	Tags                   map[string][]Tag                `json:"tags"`
	ParameterGroups        map[string]*DBParameterGroup    `json:"parameterGroups"`
	ClusterParameterGroups map[string]*DBParameterGroup    `json:"clusterParameterGroups"`
	OptionGroups           map[string]*OptionGroup         `json:"optionGroups"`
	Clusters               map[string]*DBCluster           `json:"clusters"`
	ClusterSnapshots       map[string]*DBClusterSnapshot   `json:"clusterSnapshots"`
	ClusterEndpoints       map[string]*DBClusterEndpoint   `json:"clusterEndpoints"`
	ExportTasks            map[string]*ExportTask          `json:"exportTasks"`
	GlobalClusters         map[string]*GlobalCluster       `json:"globalClusters"`
	ClusterRoles           map[string][]string             `json:"clusterRoles"`
	InstanceRoles          map[string][]string             `json:"instanceRoles"`
	EventSubscriptions     map[string]*EventSubscription   `json:"eventSubscriptions"`
	DBSecurityGroups       map[string]*DBSecurityGroup     `json:"dbSecurityGroups"`
	BlueGreenDeployments   map[string]*BlueGreenDeployment `json:"blueGreenDeployments"`
	AccountID              string                          `json:"accountID"`
	Region                 string                          `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:              b.instances,
		Snapshots:              b.snapshots,
		SubnetGroups:           b.subnetGroups,
		Tags:                   b.tags,
		ParameterGroups:        b.parameterGroups,
		ClusterParameterGroups: b.clusterParameterGroups,
		OptionGroups:           b.optionGroups,
		Clusters:               b.clusters,
		ClusterSnapshots:       b.clusterSnapshots,
		ClusterEndpoints:       b.clusterEndpoints,
		ExportTasks:            b.exportTasks,
		GlobalClusters:         b.globalClusters,
		ClusterRoles:           b.clusterRoles,
		InstanceRoles:          b.instanceRoles,
		EventSubscriptions:     b.eventSubscriptions,
		DBSecurityGroups:       b.dbSecurityGroups,
		BlueGreenDeployments:   b.blueGreenDeployments,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
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
	b.accountID = snap.AccountID
	b.region = snap.Region
	// FIS fault state is transient — clear it on restore so stale faults are not retained.
	b.fisFailoverFaults = make(map[string]time.Time)

	return nil
}

// ensureNonNilMaps initialises any nil maps in a deserialized snapshot so the backend
// never operates on nil maps after a restore.
func ensureNonNilMaps(snap *backendSnapshot) {
	ensureNonNilCoreMaps(snap)
	ensureNonNilExtendedMaps(snap)
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
}

// ensureNonNilExtendedMaps initialises the extended resource maps added later
// (cluster/instance roles, event subscriptions, security groups, blue/green deployments).
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
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
