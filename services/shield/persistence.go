package shield

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Protections  map[string]*Protection `json:"protections"`
	Subscription *Subscription          `json:"subscription,omitempty"`
	AccountID    string                 `json:"accountID"`
	Region       string                 `json:"region"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Protections == nil {
		s.Protections = make(map[string]*Protection)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Protections:  b.protections,
		Subscription: b.subscription,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("shield: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds indexes.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.protections = snap.Protections
	b.subscription = snap.Subscription
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild O(1) indexes.
	b.resourceARNIndex = make(map[string]string, len(snap.Protections))
	b.nameIndex = make(map[string]string, len(snap.Protections))

	for id, p := range snap.Protections {
		b.resourceARNIndex[p.ResourceARN] = id
		b.nameIndex[p.Name] = id
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
