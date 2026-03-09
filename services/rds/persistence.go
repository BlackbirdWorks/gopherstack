package rds

import (
	"encoding/json"
	"time"
)

type backendSnapshot struct {
	Instances              map[string]*DBInstance        `json:"instances"`
	Snapshots              map[string]*DBSnapshot        `json:"snapshots"`
	SubnetGroups           map[string]*DBSubnetGroup     `json:"subnetGroups"`
	Tags                   map[string][]Tag              `json:"tags"`
	ParameterGroups        map[string]*DBParameterGroup  `json:"parameterGroups"`
	ClusterParameterGroups map[string]*DBParameterGroup  `json:"clusterParameterGroups"`
	OptionGroups           map[string]*OptionGroup       `json:"optionGroups"`
	Clusters               map[string]*DBCluster         `json:"clusters"`
	ClusterSnapshots       map[string]*DBClusterSnapshot `json:"clusterSnapshots"`
	AccountID              string                        `json:"accountID"`
	Region                 string                        `json:"region"`
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

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

	b.instances = snap.Instances
	b.snapshots = snap.Snapshots
	b.subnetGroups = snap.SubnetGroups
	b.tags = snap.Tags
	b.parameterGroups = snap.ParameterGroups
	b.clusterParameterGroups = snap.ClusterParameterGroups
	b.optionGroups = snap.OptionGroups
	b.clusters = snap.Clusters
	b.clusterSnapshots = snap.ClusterSnapshots
	b.accountID = snap.AccountID
	b.region = snap.Region
	// FIS fault state is transient — clear it on restore so stale faults are not retained.
	b.fisFailoverFaults = make(map[string]time.Time)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
