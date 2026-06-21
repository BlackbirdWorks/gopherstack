package sagemakerruntime

import (
	"context"
	"encoding/json"
)

type backendSnapshot struct {
	Sessions         map[string]*Session         `json:"sessions"`
	AsyncInvocations map[string]*AsyncInvocation `json:"asyncInvocations"`
	Invocations      []*Invocation               `json:"invocations"`
	NextID           uint64                      `json:"nextId"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	invCopy := make([]*Invocation, len(b.invocations))
	for i, inv := range b.invocations {
		cp := *inv
		invCopy[i] = &cp
	}

	sessionCopy := make(map[string]*Session, len(b.sessions))
	for id, session := range b.sessions {
		sessionCopy[id] = cloneSession(session)
	}

	asyncCopy := make(map[string]*AsyncInvocation, len(b.asyncInvocations))
	for id, invocation := range b.asyncInvocations {
		asyncCopy[id] = cloneAsyncInvocation(invocation)
	}

	snap := backendSnapshot{
		Invocations:      invCopy,
		Sessions:         sessionCopy,
		AsyncInvocations: asyncCopy,
		NextID:           b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Invocations == nil {
		snap.Invocations = make([]*Invocation, 0)
	}
	if snap.Sessions == nil {
		snap.Sessions = make(map[string]*Session)
	}
	if snap.AsyncInvocations == nil {
		snap.AsyncInvocations = make(map[string]*AsyncInvocation)
	}

	b.invocations = snap.Invocations
	b.sessions = snap.Sessions
	b.asyncInvocations = snap.AsyncInvocations
	b.nextID = snap.NextID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
