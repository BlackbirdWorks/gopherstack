package cloudwatch

import (
	"encoding/json"
)

type backendSnapshot struct {
	Metrics   map[string]map[string][]MetricDatum `json:"metrics"`
	Alarms    map[string]*MetricAlarm             `json:"alarms"`
	AccountID string                              `json:"accountID"`
	Region    string                              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Metrics:   b.metrics,
		Alarms:    b.alarms,
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

	if snap.Metrics == nil {
		snap.Metrics = make(map[string]map[string][]MetricDatum)
	}

	if snap.Alarms == nil {
		snap.Alarms = make(map[string]*MetricAlarm)
	}

	b.metrics = snap.Metrics
	b.alarms = snap.Alarms
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
