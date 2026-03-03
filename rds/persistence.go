package rds

import (
	"encoding/json"
)

type backendSnapshot struct {
	Instances    map[string]*DBInstance    `json:"instances"`
	Snapshots    map[string]*DBSnapshot    `json:"snapshots"`
	SubnetGroups map[string]*DBSubnetGroup `json:"subnetGroups"`
	Tags         map[string][]Tag          `json:"tags"`
	AccountID    string                    `json:"accountID"`
	Region       string                    `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:    b.instances,
		Snapshots:    b.snapshots,
		SubnetGroups: b.subnetGroups,
		Tags:         b.tags,
		AccountID:    b.accountID,
		Region:       b.region,
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

	b.instances = snap.Instances
	b.snapshots = snap.Snapshots
	b.subnetGroups = snap.SubnetGroups
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
