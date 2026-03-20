package ce

import "encoding/json"

type backendSnapshot struct {
	CostCategories       map[string]*CostCategory        `json:"costCategories"`
	AnomalyMonitors      map[string]*AnomalyMonitor      `json:"anomalyMonitors"`
	AnomalySubscriptions map[string]*AnomalySubscription `json:"anomalySubscriptions"`
	AccountID            string                          `json:"accountID"`
	Region               string                          `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		CostCategories:       b.costCategories,
		AnomalyMonitors:      b.anomalyMonitors,
		AnomalySubscriptions: b.anomalySubscriptions,
		AccountID:            b.accountID,
		Region:               b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.CostCategories == nil {
		snap.CostCategories = make(map[string]*CostCategory)
	}

	if snap.AnomalyMonitors == nil {
		snap.AnomalyMonitors = make(map[string]*AnomalyMonitor)
	}

	if snap.AnomalySubscriptions == nil {
		snap.AnomalySubscriptions = make(map[string]*AnomalySubscription)
	}

	b.costCategories = snap.CostCategories
	b.anomalyMonitors = snap.AnomalyMonitors
	b.anomalySubscriptions = snap.AnomalySubscriptions
	b.accountID = snap.AccountID
	b.region = snap.Region

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
