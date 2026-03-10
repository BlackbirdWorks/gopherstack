package codeartifact

import (
	"encoding/json"
)

type backendSnapshot struct {
	Domains      map[string]*Domain     `json:"domains"`
	Repositories map[string]*Repository `json:"repositories"`
	AccountID    string                 `json:"accountID"`
	Region       string                 `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:      b.domains,
		Repositories: b.repositories,
		AccountID:    b.accountID,
		Region:       b.region,
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

	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}
	if snap.Repositories == nil {
		snap.Repositories = make(map[string]*Repository)
	}

	b.domains = snap.Domains
	b.repositories = snap.Repositories
	b.accountID = snap.AccountID
	b.region = snap.Region

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
