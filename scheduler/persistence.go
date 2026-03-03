package scheduler

import (
	"encoding/json"
)

type backendSnapshot struct {
	Schedules map[string]*Schedule `json:"schedules"`
	AccountID string               `json:"accountID"`
	Region    string               `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Schedules: b.schedules,
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

	if snap.Schedules == nil {
		snap.Schedules = make(map[string]*Schedule)
	}

	b.schedules = snap.Schedules
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
