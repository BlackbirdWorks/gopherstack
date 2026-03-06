package ecr

import "encoding/json"

type backendSnapshot struct {
	Repos map[string]*Repository `json:"repos"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy the repos map to ensure snapshot isolation.
	repos := make(map[string]*Repository, len(b.repos))
	for k, v := range b.repos {
		cp := *v
		repos[k] = &cp
	}

	snap := backendSnapshot{Repos: repos}

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

	if snap.Repos == nil {
		snap.Repos = make(map[string]*Repository)
	}

	b.repos = snap.Repos

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
