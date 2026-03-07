package cloudformation

import "encoding/json"

type backendSnapshot struct {
	Stacks     map[string]*Stack                    `json:"stacks"`
	Events     map[string][]StackEvent              `json:"events"`
	Resources  map[string]map[string]*StackResource `json:"resources"`
	ChangeSets map[string]map[string]*ChangeSet     `json:"changeSets"`
	Exports    map[string]*Export                   `json:"exports"`
	AccountID  string                               `json:"accountID"`
	Region     string                               `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Stacks:     b.stacks,
		Events:     b.events,
		Resources:  b.resources,
		ChangeSets: b.changeSets,
		Exports:    b.exports,
		AccountID:  b.accountID,
		Region:     b.region,
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

	if snap.Stacks == nil {
		snap.Stacks = make(map[string]*Stack)
	}

	if snap.Events == nil {
		snap.Events = make(map[string][]StackEvent)
	}

	if snap.Resources == nil {
		snap.Resources = make(map[string]map[string]*StackResource)
	}

	if snap.ChangeSets == nil {
		snap.ChangeSets = make(map[string]map[string]*ChangeSet)
	}

	if snap.Exports == nil {
		snap.Exports = make(map[string]*Export)
	}

	b.stacks = snap.Stacks
	b.events = snap.Events
	b.resources = snap.Resources
	b.changeSets = snap.ChangeSets
	b.exports = snap.Exports
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
