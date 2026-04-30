package docdb

import (
	"encoding/json"
	"log/slog"
)

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Clusters               map[string]*DBCluster                         `json:"clusters"`
	Instances              map[string]*DBInstance                        `json:"instances"`
	SubnetGroups           map[string]*DBSubnetGroup                     `json:"subnetGroups"`
	ClusterParameterGroups map[string]*DBClusterParameterGroup           `json:"clusterParameterGroups"`
	ClusterSnapshots       map[string]*DBClusterSnapshot                 `json:"clusterSnapshots"`
	SnapshotAttributes     map[string]*DBClusterSnapshotAttributesResult `json:"snapshotAttributes"`
	EventSubscriptions     map[string]*EventSubscription                 `json:"eventSubscriptions"`
	GlobalClusters         map[string]*GlobalCluster                     `json:"globalClusters"`
	Tags                   map[string][]Tag                              `json:"tags"`
	AccountID              string                                        `json:"accountID"`
	Region                 string                                        `json:"region"`
}

// ensureNonNil initialises any nil maps so callers do not need to guard after Restore.
func (s *backendSnapshot) ensureNonNil() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*DBCluster)
	}

	if s.Instances == nil {
		s.Instances = make(map[string]*DBInstance)
	}

	if s.SubnetGroups == nil {
		s.SubnetGroups = make(map[string]*DBSubnetGroup)
	}

	if s.ClusterParameterGroups == nil {
		s.ClusterParameterGroups = make(map[string]*DBClusterParameterGroup)
	}

	if s.ClusterSnapshots == nil {
		s.ClusterSnapshots = make(map[string]*DBClusterSnapshot)
	}

	if s.SnapshotAttributes == nil {
		s.SnapshotAttributes = make(map[string]*DBClusterSnapshotAttributesResult)
	}

	if s.EventSubscriptions == nil {
		s.EventSubscriptions = make(map[string]*EventSubscription)
	}

	if s.GlobalClusters == nil {
		s.GlobalClusters = make(map[string]*GlobalCluster)
	}

	if s.Tags == nil {
		s.Tags = make(map[string][]Tag)
	}
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot() []byte {
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
		slog.Default().Warn("docdb: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

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
