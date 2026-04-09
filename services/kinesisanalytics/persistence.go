package kinesisanalytics

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Apps   map[string]*Application `json:"apps"`
	NextID int64                   `json:"next_id"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	appsCopy := make(map[string]*Application, len(b.apps))
	for k, v := range b.apps {
		appsCopy[k] = appCopy(v)
	}

	snap := backendSnapshot{
		Apps:   appsCopy,
		NextID: b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("kinesisanalytics: failed to marshal snapshot", "error", err)

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

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Apps == nil {
		snap.Apps = make(map[string]*Application)
	}

	b.apps = snap.Apps
	b.nextID = snap.NextID

	// Rebuild ARN index from restored applications.
	b.appsByARN = make(map[string]*Application, len(b.apps))
	for _, app := range b.apps {
		b.appsByARN[app.ApplicationARN] = app
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(data)
	}

	return nil
}
