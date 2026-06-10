package cloudwatch

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Metrics          map[string]map[string]*metricRecord `json:"metrics"`
	Alarms           map[string]*MetricAlarm             `json:"alarms"`
	CompositeAlarms  map[string]*CompositeAlarm          `json:"compositeAlarms"`
	AlarmHistory     map[string][]AlarmHistoryItem       `json:"alarmHistory"`
	Dashboards       map[string]*dashboardRecord         `json:"dashboards"`
	AnomalyDetectors map[string]*AnomalyDetector         `json:"anomalyDetectors"`
	InsightRules     map[string]*InsightRule             `json:"insightRules"`
	MetricStreams    map[string]*MetricStream            `json:"metricStreams"`
	AlarmMuteRules   map[string]*AlarmMuteRule           `json:"alarmMuteRules"`
	MetricFilters    map[string]*MetricFilter            `json:"metricFilters"`
	AccountID        string                              `json:"accountID"`
	Region           string                              `json:"region"`
	TotalMetrics     int                                 `json:"totalMetrics"`
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
		Dashboards:       b.dashboards,
		AnomalyDetectors: b.anomalyDetectors,
		InsightRules:     b.insightRules,
		MetricStreams:    b.metricStreams,
		AlarmMuteRules:   b.alarmMuteRules,
		MetricFilters:    b.metricFilters,
		AccountID:        b.accountID,
		Region:           b.region,
		TotalMetrics:     b.totalMetrics,
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
		snap.Metrics = make(map[string]map[string]*metricRecord)
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

	if snap.Dashboards == nil {
		snap.Dashboards = make(map[string]*dashboardRecord)
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

	if snap.MetricFilters == nil {
		snap.MetricFilters = make(map[string]*MetricFilter)
	}

	b.metrics = snap.Metrics
	b.alarms = snap.Alarms
	b.compositeAlarms = snap.CompositeAlarms
	b.alarmHistory = snap.AlarmHistory
	b.dashboards = snap.Dashboards
	b.anomalyDetectors = snap.AnomalyDetectors
	b.insightRules = snap.InsightRules
	b.metricStreams = snap.MetricStreams
	b.alarmMuteRules = snap.AlarmMuteRules
	b.metricFilters = snap.MetricFilters
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.totalMetrics = snap.TotalMetrics

	// #60: recompute running total from restored metrics.
	total := 0
	for _, nsMap := range b.metrics {
		total += len(nsMap)
	}

	b.totalMetrics = total

	return nil
}

// handlerSnapshot wraps the backend snapshot with the handler-level tags map.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend []byte                       `json:"backend"`
}

// Snapshot implements persistence.Persistable by delegating to the backend and
// also persisting the handler-level resource tags.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }

	var backendData []byte
	if s, ok := h.Backend.(snapshotter); ok {
		backendData = s.Snapshot()
	}

	h.tagsMu.RLock("Snapshot")
	tagsCopy := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		if t != nil {
			tagsCopy[k] = t.Clone()
		}
	}
	h.tagsMu.RUnlock()

	data, err := json.Marshal(handlerSnapshot{Backend: backendData, Tags: tagsCopy})
	if err != nil {
		return backendData
	}

	return data
}

// Restore implements persistence.Persistable by delegating to the backend and
// restoring the handler-level resource tags.
func (h *Handler) Restore(data []byte) error {
	// Attempt to unmarshal as a handlerSnapshot first.
	var hs handlerSnapshot
	if unmarshalErr := json.Unmarshal(data, &hs); unmarshalErr == nil && hs.Backend != nil {
		type restorer interface{ Restore([]byte) error }
		if r, ok := h.Backend.(restorer); ok {
			if err := r.Restore(hs.Backend); err != nil {
				return err
			}
		}

		h.tagsMu.Lock("Restore")
		h.tags = make(map[string]*tags.Tags, len(hs.Tags))
		for k, kv := range hs.Tags {
			h.tags[k] = tags.FromMap("cloudwatch."+k+".tags", kv)
		}
		h.tagsMu.Unlock()

		return nil
	}

	// Fall back: treat data as a bare backend snapshot (backward compatibility).
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
