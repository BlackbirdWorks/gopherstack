package ses

import (
	"encoding/json"
	"maps"
)

type backendSnapshot struct {
	Identities map[string]bool `json:"identities"`
	Emails     []Email         `json:"emails"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	ids := make(map[string]bool, len(b.identities))
	maps.Copy(ids, b.identities)

	emails := make([]Email, len(b.emails))
	copy(emails, b.emails)

	snap := backendSnapshot{
		Identities: ids,
		Emails:     emails,
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

	if snap.Identities == nil {
		snap.Identities = make(map[string]bool)
	}

	if snap.Emails == nil {
		snap.Emails = []Email{}
	}

	b.identities = snap.Identities
	b.emails = snap.Emails

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
