package ram

import "encoding/json"

type backendSnapshot struct {
	ResourceShares map[string]*ResourceShare   `json:"resourceShares"`
	AccountID      string                      `json:"accountID"`
	Region         string                      `json:"region"`
	Associations   []*ResourceShareAssociation `json:"associations"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ResourceShares: b.resourceShares,
		Associations:   b.associations,
		AccountID:      b.accountID,
		Region:         b.region,
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

	if snap.ResourceShares == nil {
		snap.ResourceShares = make(map[string]*ResourceShare)
	}

	if snap.Associations == nil {
		snap.Associations = make([]*ResourceShareAssociation, 0)
	}

	b.resourceShares = snap.ResourceShares
	b.associations = snap.Associations
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
