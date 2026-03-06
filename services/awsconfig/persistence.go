package awsconfig

import (
	"encoding/json"
)

type backendSnapshot struct {
	Recorders map[string]*ConfigurationRecorder `json:"recorders"`
	Channels  map[string]*DeliveryChannel       `json:"channels"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Recorders: b.recorders,
		Channels:  b.channels,
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

	if snap.Recorders == nil {
		snap.Recorders = make(map[string]*ConfigurationRecorder)
	}

	if snap.Channels == nil {
		snap.Channels = make(map[string]*DeliveryChannel)
	}

	b.recorders = snap.Recorders
	b.channels = snap.Channels

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
