package cloudwatchlogs

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Groups              map[string]*LogGroup                    `json:"groups"`
	Streams             map[string]map[string]*LogStream        `json:"streams"`
	Events              map[string]map[string][]*OutputLogEvent `json:"events"`
	SubscriptionFilters map[string][]*SubscriptionFilter        `json:"subscriptionFilters"`
	ExportTasks         map[string]*ExportTask                  `json:"exportTasks,omitempty"`
	ImportTasks         map[string]*ImportTask                  `json:"importTasks,omitempty"`
	Deliveries          map[string]*Delivery                    `json:"deliveries,omitempty"`
	LogAnomalyDetectors map[string]*LogAnomalyDetector          `json:"logAnomalyDetectors,omitempty"`
	ScheduledQueries    map[string]*ScheduledQuery              `json:"scheduledQueries,omitempty"`
	AccountPolicies     map[string]*AccountPolicy               `json:"accountPolicies,omitempty"`
	KmsKeys             map[string]string                       `json:"kmsKeys,omitempty"`
	S3TableIntegrations map[string]string                       `json:"s3TableIntegrations,omitempty"`
	AccountID           string                                  `json:"accountID"`
	Region              string                                  `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:              b.groups,
		Streams:             b.streams,
		Events:              b.events,
		SubscriptionFilters: b.subscriptionFilters,
		ExportTasks:         b.exportTasks,
		ImportTasks:         b.importTasks,
		Deliveries:          b.deliveries,
		LogAnomalyDetectors: b.logAnomalyDetectors,
		ScheduledQueries:    b.scheduledQueries,
		AccountPolicies:     b.accountPolicies,
		KmsKeys:             b.kmsKeys,
		S3TableIntegrations: b.s3TableIntegrations,
		AccountID:           b.accountID,
		Region:              b.region,
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

	if snap.Groups == nil {
		snap.Groups = make(map[string]*LogGroup)
	}

	if snap.Streams == nil {
		snap.Streams = make(map[string]map[string]*LogStream)
	}

	if snap.Events == nil {
		snap.Events = make(map[string]map[string][]*OutputLogEvent)
	}

	if snap.SubscriptionFilters == nil {
		snap.SubscriptionFilters = make(map[string][]*SubscriptionFilter)
	}

	if snap.ExportTasks == nil {
		snap.ExportTasks = make(map[string]*ExportTask)
	}

	if snap.ImportTasks == nil {
		snap.ImportTasks = make(map[string]*ImportTask)
	}

	if snap.Deliveries == nil {
		snap.Deliveries = make(map[string]*Delivery)
	}

	if snap.LogAnomalyDetectors == nil {
		snap.LogAnomalyDetectors = make(map[string]*LogAnomalyDetector)
	}

	if snap.ScheduledQueries == nil {
		snap.ScheduledQueries = make(map[string]*ScheduledQuery)
	}

	if snap.AccountPolicies == nil {
		snap.AccountPolicies = make(map[string]*AccountPolicy)
	}

	if snap.KmsKeys == nil {
		snap.KmsKeys = make(map[string]string)
	}

	if snap.S3TableIntegrations == nil {
		snap.S3TableIntegrations = make(map[string]string)
	}

	b.groups = snap.Groups
	b.streams = snap.Streams
	b.events = snap.Events
	b.subscriptionFilters = snap.SubscriptionFilters
	b.exportTasks = snap.ExportTasks
	b.importTasks = snap.ImportTasks
	b.deliveries = snap.Deliveries
	b.logAnomalyDetectors = snap.LogAnomalyDetectors
	b.scheduledQueries = snap.ScheduledQueries
	b.accountPolicies = snap.AccountPolicies
	b.kmsKeys = snap.KmsKeys
	b.s3TableIntegrations = snap.S3TableIntegrations
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// handlerSnapshot is the full persisted state for a Handler, combining both
// backend state and the handler-level tag data that lives outside the backend.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend []byte                       `json:"backend"`
}

// Snapshot implements persistence.Persistable by serialising both the backend
// state and the handler-owned tag data.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }

	var backendData []byte
	if s, ok := h.Backend.(snapshotter); ok {
		backendData = s.Snapshot()
	}

	// Collect tags outside the backend lock.
	h.tagsMu.RLock("Snapshot")
	tagMap := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		tagMap[k] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Backend: backendData,
		Tags:    tagMap,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore implements persistence.Persistable by restoring both the backend
// state and the handler-owned tag data.
func (h *Handler) Restore(data []byte) error {
	// Attempt to decode as the combined handlerSnapshot format first.
	var snap handlerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if err := h.restoreBackend(snap.Backend, data); err != nil {
		return err
	}

	h.restoreTags(snap.Tags)

	return nil
}

// restoreBackend restores backend state from the snapshot.
// If backendData is non-nil it came from the new combined format; otherwise the
// caller should fall back to the raw data (legacy bare-backend format).
func (h *Handler) restoreBackend(backendData, rawData []byte) error {
	type restorer interface{ Restore([]byte) error }

	r, ok := h.Backend.(restorer)
	if !ok {
		return nil
	}

	src := backendData
	if src == nil {
		src = rawData
	}

	return r.Restore(src)
}

// restoreTags replaces the handler's tag store with the persisted tag map.
// All existing tags are discarded and replaced with the snapshot values.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	// Close existing tag collections to prevent Prometheus metric registry leaks.
	for _, t := range h.tags {
		t.Close()
	}

	// Replace with a fresh map seeded from the snapshot.
	h.tags = make(map[string]*tags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := tags.New("cwl." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
