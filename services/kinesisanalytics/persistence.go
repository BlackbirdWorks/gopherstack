package kinesisanalytics

import (
	"encoding/json"
	"maps"
)

type backendSnapshot struct {
	Apps map[string]*Application `json:"apps"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	appsCopy := make(map[string]*Application, len(b.apps))
	for k, v := range b.apps {
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		appsCopy[k] = &cp
	}

	snap := backendSnapshot{Apps: appsCopy}

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

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Apps == nil {
		snap.Apps = make(map[string]*Application)
	}

	b.apps = snap.Apps

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
