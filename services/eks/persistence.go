package eks

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Clusters   map[string]*Cluster              `json:"clusters"`
	Nodegroups map[string]map[string]*Nodegroup `json:"nodegroups"`
	AccountID  string                           `json:"accountId"`
	Region     string                           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:   b.clusters,
		Nodegroups: b.nodegroups,
		AccountID:  b.accountID,
		Region:     b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	if snap.Nodegroups == nil {
		snap.Nodegroups = make(map[string]map[string]*Nodegroup)
	}

	// Rebuild tags with proper Prometheus lock names. Tags deserialized from
	// JSON use the generic "json.tags" name; we reassign to named instances.
	for name, c := range snap.Clusters {
		rawTags := c.Tags.Clone()
		c.Tags.Close()
		c.Tags = tags.FromMap("eks.cluster."+name+".tags", rawTags)
	}

	for clusterName, ngs := range snap.Nodegroups {
		if ngs == nil {
			snap.Nodegroups[clusterName] = make(map[string]*Nodegroup)

			continue
		}

		for ngName, ng := range ngs {
			rawTags := ng.Tags.Clone()
			ng.Tags.Close()
			ng.Tags = tags.FromMap("eks.nodegroup."+clusterName+"."+ngName+".tags", rawTags)
		}
	}

	b.clusters = snap.Clusters
	b.nodegroups = snap.Nodegroups
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
