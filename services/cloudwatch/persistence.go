package cloudwatch

import (
	"encoding/json"
)

type backendSnapshot struct {
	Metrics          map[string]map[string][]MetricDatum `json:"metrics"`
	Alarms           map[string]*MetricAlarm             `json:"alarms"`
	CompositeAlarms  map[string]*CompositeAlarm          `json:"compositeAlarms"`
	AlarmHistory     map[string][]AlarmHistoryItem       `json:"alarmHistory"`
	AnomalyDetectors map[string]*AnomalyDetector         `json:"anomalyDetectors"`
	InsightRules     map[string]*InsightRule             `json:"insightRules"`
	MetricStreams    map[string]*MetricStream            `json:"metricStreams"`
	AlarmMuteRules   map[string]*AlarmMuteRule           `json:"alarmMuteRules"`
	AccountID        string                              `json:"accountID"`
	Region           string                              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Metrics:          b.metrics,
		Alarms:           b.alarms,
		CompositeAlarms:  b.compositeAlarms,
		AlarmHistory:     b.alarmHistory,
		AnomalyDetectors: b.anomalyDetectors,
		InsightRules:     b.insightRules,
		MetricStreams:    b.metricStreams,
		AlarmMuteRules:   b.alarmMuteRules,
		AccountID:        b.accountID,
		Region:           b.region,
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

	if snap.CompositeAlarms == nil {
		snap.CompositeAlarms = make(map[string]*CompositeAlarm)
	}

	if snap.AlarmHistory == nil {
		snap.AlarmHistory = make(map[string][]AlarmHistoryItem)
	}

	if snap.AnomalyDetectors == nil {
		snap.AnomalyDetectors = make(map[string]*AnomalyDetector)
	}

	if snap.InsightRules == nil {
		snap.InsightRules = make(map[string]*InsightRule)
	}

	if snap.MetricStreams == nil {
		snap.MetricStreams = make(map[string]*MetricStream)
	}

	if snap.AlarmMuteRules == nil {
		snap.AlarmMuteRules = make(map[string]*AlarmMuteRule)
	}

	b.metrics = snap.Metrics
	b.alarms = snap.Alarms
	b.compositeAlarms = snap.CompositeAlarms
	b.alarmHistory = snap.AlarmHistory
	b.anomalyDetectors = snap.AnomalyDetectors
	b.insightRules = snap.InsightRules
	b.metricStreams = snap.MetricStreams
	b.alarmMuteRules = snap.AlarmMuteRules
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
