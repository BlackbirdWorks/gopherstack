package docdb

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot persists the backend state. Regional resource maps are nested by
// region (outer key = region). GlobalClusters are partition-scoped and stay flat.
type backendSnapshot struct {
	Clusters               map[string]map[string]*DBCluster                         `json:"clusters"`
	Instances              map[string]map[string]*DBInstance                        `json:"instances"`
	SubnetGroups           map[string]map[string]*DBSubnetGroup                     `json:"subnetGroups"`
	ClusterParameterGroups map[string]map[string]*DBClusterParameterGroup           `json:"clusterParameterGroups"`
	ClusterSnapshots       map[string]map[string]*DBClusterSnapshot                 `json:"clusterSnapshots"`
	SnapshotAttributes     map[string]map[string]*DBClusterSnapshotAttributesResult `json:"snapshotAttributes"`
	EventSubscriptions     map[string]map[string]*EventSubscription                 `json:"eventSubscriptions"`
	GlobalClusters         map[string]*GlobalCluster                                `json:"globalClusters"`
	Tags                   map[string]map[string][]Tag                              `json:"tags"`
	AccountID              string                                                   `json:"accountID"`
	Region                 string                                                   `json:"region"`
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

	if snap.SnapshotAttributes == nil {
		snap.SnapshotAttributes = make(map[string]map[string]*DBClusterSnapshotAttributesResult)
	}

	if snap.EventSubscriptions == nil {
		snap.EventSubscriptions = make(map[string]map[string]*EventSubscription)
	}

	if snap.GlobalClusters == nil {
		snap.GlobalClusters = make(map[string]*GlobalCluster)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string][]Tag)
	}
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:               b.clusters,
		Instances:              b.instances,
		SubnetGroups:           b.subnetGroups,
		ClusterParameterGroups: b.clusterParameterGroups,
		ClusterSnapshots:       b.clusterSnapshots,
		SnapshotAttributes:     b.snapshotAttributes,
		EventSubscriptions:     b.eventSubscriptions,
		GlobalClusters:         b.globalClusters,
		Tags:                   b.tags,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "docdb: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "docdb", data, &snap); err != nil {
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
	b.snapshotAttributes = snap.SnapshotAttributes
	b.eventSubscriptions = snap.EventSubscriptions
	b.globalClusters = snap.GlobalClusters
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
