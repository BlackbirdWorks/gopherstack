package sts

import (
	"encoding/json"
	"time"
)

type backendSnapshot struct {
	Sessions map[string]*SessionInfo `json:"sessions"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	snap := backendSnapshot{
		Sessions: b.sessions,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// Expired sessions are discarded on load.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	now := time.Now().UTC()

	b.mu.Lock()
	defer b.mu.Unlock()

	b.sessions = make(map[string]*SessionInfo)

	for k, s := range snap.Sessions {
		if s == nil {
			continue
		}

		// Discard already-expired sessions; keep zero-valued (non-expiring) sessions.
		if !s.Expiration.IsZero() && !now.Before(s.Expiration) {
			continue
		}

		b.sessions[k] = s
	}

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
