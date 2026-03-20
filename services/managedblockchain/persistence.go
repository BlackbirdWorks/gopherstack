package managedblockchain

import "encoding/json"

type backendSnapshot struct {
	Networks map[string]*Network               `json:"networks"`
	Members  map[string]map[string]*Member     `json:"members"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Networks: b.networks,
		Members:  b.members,
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

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Networks == nil {
		snap.Networks = make(map[string]*Network)
	}

	if snap.Members == nil {
		snap.Members = make(map[string]map[string]*Member)
	}

	b.networks = snap.Networks
	b.members = snap.Members

	// Rebuild ARN index from restored state.
	b.arnToResource = make(map[string]interface{})

	for _, n := range b.networks {
		b.arnToResource[n.Arn] = n
	}

	for _, memberMap := range b.members {
		for _, m := range memberMap {
			b.arnToResource[m.Arn] = m
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(data)
	}

	return nil
}
