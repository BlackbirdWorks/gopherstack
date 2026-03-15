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

	b.groups = snap.Groups
	b.streams = snap.Streams
	b.events = snap.Events
	b.subscriptionFilters = snap.SubscriptionFilters
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

// restoreTags merges the persisted tag map back into the handler's tag store.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	if len(tagMap) == 0 {
		return
	}

	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for resourceID, kv := range tagMap {
		if h.tags[resourceID] == nil {
			h.tags[resourceID] = tags.New("cwl." + resourceID + ".tags")
		}

		h.tags[resourceID].Merge(kv)
	}
}
