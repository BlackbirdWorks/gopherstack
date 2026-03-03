package ssm

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Parameters map[string]Parameter          `json:"parameters"`
	History    map[string][]ParameterHistory `json:"history"`
	Tags       map[string]*tags.Tags         `json:"tags"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Parameters: b.parameters,
		History:    b.history,
		Tags:       b.tags,
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

	if snap.Parameters == nil {
		snap.Parameters = make(map[string]Parameter)
	}

	if snap.History == nil {
		snap.History = make(map[string][]ParameterHistory)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]*tags.Tags)
	}

	b.parameters = snap.Parameters
	b.history = snap.History
	b.tags = snap.Tags

	return nil
}
