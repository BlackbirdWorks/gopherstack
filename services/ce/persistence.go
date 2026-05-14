package ce

import (
	"encoding/json"
	"time"
)

type backendSnapshot struct {
	CostCategories       map[string]*CostCategory        `json:"costCategories"`
	AnomalyMonitors      map[string]*AnomalyMonitor      `json:"anomalyMonitors"`
	AnomalySubscriptions map[string]*AnomalySubscription `json:"anomalySubscriptions"`
	Anomalies            map[string]*Anomaly             `json:"anomalies"`
	AccountID            string                          `json:"accountID"`
	Region               string                          `json:"region"`
	AnomalyTTL           time.Duration                   `json:"anomalyTTL"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		CostCategories:       b.costCategories,
		AnomalyMonitors:      b.anomalyMonitors,
		AnomalySubscriptions: b.anomalySubscriptions,
		Anomalies:            b.anomalies,
		AccountID:            b.accountID,
		Region:               b.region,
		AnomalyTTL:           b.anomalyTTL,
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

	if snap.Anomalies == nil {
		snap.Anomalies = make(map[string]*Anomaly)
	}

	b.costCategories = snap.CostCategories
	b.anomalyMonitors = snap.AnomalyMonitors
	b.anomalySubscriptions = snap.AnomalySubscriptions
	b.anomalies = snap.Anomalies
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Default to DefaultAnomalyTTL when restoring from a pre-AnomalyTTL snapshot
	// to avoid evicting every anomaly on Restore (zero TTL is "evict everything").
	if snap.AnomalyTTL > 0 {
		b.anomalyTTL = snap.AnomalyTTL
	} else {
		b.anomalyTTL = DefaultAnomalyTTL
	}

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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
