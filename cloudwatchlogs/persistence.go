package cloudwatchlogs

import (
	"encoding/json"
)

type backendSnapshot struct {
	Groups    map[string]*LogGroup                    `json:"groups"`
	Streams   map[string]map[string]*LogStream        `json:"streams"`
	Events    map[string]map[string][]*OutputLogEvent `json:"events"`
	AccountID string                                  `json:"accountID"`
	Region    string                                  `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:    b.groups,
		Streams:   b.streams,
		Events:    b.events,
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

	if snap.Groups == nil {
		snap.Groups = make(map[string]*LogGroup)
	}

	if snap.Streams == nil {
		snap.Streams = make(map[string]map[string]*LogStream)
	}

	if snap.Events == nil {
		snap.Events = make(map[string]map[string][]*OutputLogEvent)
	}

	b.groups = snap.Groups
	b.streams = snap.Streams
	b.events = snap.Events
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
