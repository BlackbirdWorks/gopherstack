package sagemakerruntime

import (
	"encoding/json"
)

type backendSnapshot struct {
	Invocations []*Invocation `json:"invocations"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	invCopy := make([]*Invocation, len(b.invocations))
	for i, inv := range b.invocations {
		cp := *inv
		invCopy[i] = &cp
	}

	snap := backendSnapshot{Invocations: invCopy}

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

	if snap.Invocations == nil {
		snap.Invocations = make([]*Invocation, 0)
	}

	b.invocations = snap.Invocations

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
