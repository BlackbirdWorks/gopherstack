package scheduler

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Schedules map[string]*Schedule `json:"schedules"`
	AccountID string               `json:"accountID"`
	Region    string               `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Schedules: b.schedules,
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

	if snap.Schedules == nil {
		snap.Schedules = make(map[string]*Schedule)
	}

	for _, s := range b.schedules {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	b.schedules = snap.Schedules
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild ARN index from restored state.
	b.scheduleARNIndex = make(map[string]string, len(b.schedules))
	for name, s := range b.schedules {
		b.scheduleARNIndex[s.ARN] = name
	}

	// Rebuild Tags with proper lockmetrics names after JSON deserialization.
	for name, s := range b.schedules {
		if s.Tags != nil {
			restored := tags.FromMap("scheduler.group."+name+".tags", s.Tags.Clone())
			s.Tags.Close()
			s.Tags = restored
		} else {
			s.Tags = tags.New("scheduler.group." + name + ".tags")
		}
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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
