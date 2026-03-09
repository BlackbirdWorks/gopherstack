package sesv2

import (
	"encoding/json"
)

type backendSnapshot struct {
	Identities        map[string]*EmailIdentity    `json:"identities"`
	ConfigurationSets map[string]*ConfigurationSet `json:"configurationSets"`
	Emails            []Email                      `json:"emails"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Identities:        b.identities,
		ConfigurationSets: b.configurationSets,
		Emails:            b.emails,
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
		snap.Identities = make(map[string]*EmailIdentity)
	}

	if snap.ConfigurationSets == nil {
		snap.ConfigurationSets = make(map[string]*ConfigurationSet)
	}

	b.identities = snap.Identities
	b.configurationSets = snap.ConfigurationSets
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
