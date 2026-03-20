package elasticsearch

import "encoding/json"

type backendSnapshot struct {
	Domains   map[string]*Domain `json:"domains"`
	AccountID string             `json:"accountID"`
	Region    string             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:   b.domains,
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

	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}

	// Close existing Tags to release Prometheus metrics before replacing state.
	for _, d := range b.domains {
		d.Tags.Close()
	}

	b.domains = snap.Domains
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild ARN index from restored state.
	b.arnIndex = make(map[string]string, len(b.domains))
	for name, d := range b.domains {
		b.arnIndex[d.ARN] = name
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
