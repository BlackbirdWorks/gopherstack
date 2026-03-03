package redshift

import (
	"encoding/json"
)

type backendSnapshot struct {
	Clusters  map[string]*Cluster `json:"clusters"`
	AccountID string              `json:"accountID"`
	Region    string              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:  b.clusters,
		AccountID: b.accountID,
		Region:    b.region,
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

	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	b.clusters = snap.Clusters
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
