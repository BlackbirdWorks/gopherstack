package eventbridge

import (
	"encoding/json"
	"log/slog"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Buses     map[string]*EventBus          `json:"buses"`
	Rules     map[string]map[string]*Rule   `json:"rules"`
	Targets   map[string]map[string]*Target `json:"targets"`
	AccountID string                        `json:"accountID"`
	Region    string                        `json:"region"`
	EventLog  []EventLogEntry               `json:"eventLog"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Buses:     b.buses,
		Rules:     b.rules,
		Targets:   b.targets,
		EventLog:  b.eventLog,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("persistence: snapshot marshal failed", "service", "eventbridge", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The logger and delivery targets are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Buses == nil {
		snap.Buses = make(map[string]*EventBus)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]map[string]*Rule)
	}

	if snap.Targets == nil {
		snap.Targets = make(map[string]map[string]*Target)
	}

	b.buses = snap.Buses
	b.rules = snap.Rules
	b.targets = snap.Targets
	b.eventLog = snap.EventLog
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
// If backendData is non-nil it came from the new combined format; otherwise
// the caller falls back to the raw data (legacy bare-backend format).
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
// All existing tag collections are closed to prevent Prometheus metric
// registry leaks, then replaced with values from the snapshot.
func (h *Handler) restoreTags(tagMap map[string]map[string]string) {
	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		t.Close()
	}

	h.tags = make(map[string]*svcTags.Tags, len(tagMap))

	for resourceID, kv := range tagMap {
		t := svcTags.New("eb." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}
