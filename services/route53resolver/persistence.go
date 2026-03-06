package route53resolver

import (
	"encoding/json"
)

type backendSnapshot struct {
	Endpoints map[string]*ResolverEndpoint `json:"endpoints"`
	Rules     map[string]*ResolverRule     `json:"rules"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Endpoints: b.endpoints,
		Rules:     b.rules,
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
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Endpoints == nil {
		snap.Endpoints = make(map[string]*ResolverEndpoint)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]*ResolverRule)
	}

	b.endpoints = snap.Endpoints
	b.rules = snap.Rules
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
