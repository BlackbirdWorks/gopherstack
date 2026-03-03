package ses

import (
	"encoding/json"
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

	snap := backendSnapshot{
		Identities: b.identities,
		Emails:     b.emails,
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

	b.identities = snap.Identities
	b.emails = snap.Emails

	return nil
}
