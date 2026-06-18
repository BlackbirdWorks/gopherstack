package neptune

import (
	"encoding/json"
	"log/slog"
)

// backendSnapshot persists the backend state. Regional resource maps are nested by
// region (outer key = region). GlobalClusters are partition-scoped and stay flat.
type backendSnapshot struct {
	Clusters               map[string]map[string]*DBCluster               `json:"clusters"`
	Instances              map[string]map[string]*DBInstance              `json:"instances"`
	SubnetGroups           map[string]map[string]*DBSubnetGroup           `json:"subnetGroups"`
	ClusterParameterGroups map[string]map[string]*DBClusterParameterGroup `json:"clusterParameterGroups"`
	ClusterSnapshots       map[string]map[string]*DBClusterSnapshot       `json:"clusterSnapshots"`
	ParameterGroups        map[string]map[string]*DBParameterGroup        `json:"parameterGroups"`
	ClusterEndpoints       map[string]map[string]*DBClusterEndpoint       `json:"clusterEndpoints"`
	EventSubscriptions     map[string]map[string]*EventSubscription       `json:"eventSubscriptions"`
	ClusterRoles           map[string]map[string][]string                 `json:"clusterRoles"`
	Tags                   map[string]map[string][]Tag                    `json:"tags"`
	GlobalClusters         map[string]*GlobalCluster                      `json:"globalClusters"`
	AccountID              string                                         `json:"accountID"`
	Region                 string                                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:               b.clusters,
		Instances:              b.instances,
		SubnetGroups:           b.subnetGroups,
		ClusterParameterGroups: b.clusterParameterGroups,
		ClusterSnapshots:       b.clusterSnapshots,
		ParameterGroups:        b.parameterGroups,
		ClusterEndpoints:       b.clusterEndpoints,
		EventSubscriptions:     b.eventSubscriptions,
		GlobalClusters:         b.globalClusters,
		ClusterRoles:           b.clusterRoles,
		Tags:                   b.tags,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("neptune: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.instances = snap.Instances
	b.subnetGroups = snap.SubnetGroups
	b.clusterParameterGroups = snap.ClusterParameterGroups
	b.clusterSnapshots = snap.ClusterSnapshots
	b.parameterGroups = snap.ParameterGroups
	b.clusterEndpoints = snap.ClusterEndpoints
	b.eventSubscriptions = snap.EventSubscriptions
	b.globalClusters = snap.GlobalClusters
	b.clusterRoles = snap.ClusterRoles
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]map[string]*DBCluster)
	}

	if snap.Instances == nil {
		snap.Instances = make(map[string]map[string]*DBInstance)
	}

	if snap.SubnetGroups == nil {
		snap.SubnetGroups = make(map[string]map[string]*DBSubnetGroup)
	}

	if snap.ClusterParameterGroups == nil {
		snap.ClusterParameterGroups = make(map[string]map[string]*DBClusterParameterGroup)
	}

	if snap.ClusterSnapshots == nil {
		snap.ClusterSnapshots = make(map[string]map[string]*DBClusterSnapshot)
	}

	if snap.ParameterGroups == nil {
		snap.ParameterGroups = make(map[string]map[string]*DBParameterGroup)
	}

	if snap.ClusterEndpoints == nil {
		snap.ClusterEndpoints = make(map[string]map[string]*DBClusterEndpoint)
	}

	if snap.EventSubscriptions == nil {
		snap.EventSubscriptions = make(map[string]map[string]*EventSubscription)
	}

	if snap.GlobalClusters == nil {
		snap.GlobalClusters = make(map[string]*GlobalCluster)
	}

	if snap.ClusterRoles == nil {
		snap.ClusterRoles = make(map[string]map[string][]string)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string][]Tag)
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
