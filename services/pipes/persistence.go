package pipes

import "encoding/json"

type backendSnapshot struct {
	Pipes     map[string]*Pipe `json:"pipes"`
	AccountID string           `json:"accountID"`
	Region    string           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Pipes:     b.pipes,
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
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Pipes == nil {
		snap.Pipes = make(map[string]*Pipe)
	}

	b.pipes = snap.Pipes
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.pipeARNIndex = make(map[string]string, len(b.pipes))

	for name, p := range b.pipes {
		b.pipeARNIndex[p.ARN] = name
	}

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
