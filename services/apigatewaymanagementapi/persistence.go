package apigatewaymanagementapi

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type persistedConn struct {
	Connection *Connection      `json:"connection"`
	Messages   []PostedMessage  `json:"messages,omitempty"`
	Events     []LifecycleEvent `json:"events,omitempty"`
}

type backendSnapshot struct {
	Connections map[string]persistedConn `json:"connections"`
	Stats       Stats                    `json:"stats"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Connections: make(map[string]persistedConn, len(b.connections)),
		Stats:       b.stats,
	}

	for id, state := range b.connections {
		snap.Connections[id] = persistedConn{
			Connection: state.conn,
			Messages:   state.msgs.snapshot(),
			Events:     append([]LifecycleEvent(nil), state.events...),
		}
	}

	return persistence.MarshalSnapshot(ctx, "apigatewaymanagementapi", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "apigatewaymanagementapi", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.connections = make(map[string]*connState, len(snap.Connections))
	b.stats = snap.Stats

	for id, persisted := range snap.Connections {
		ring := newMessageRing(maxMessagesPerConnection)
		for _, m := range persisted.Messages {
			ring.push(m)
		}

		b.connections[id] = &connState{
			conn:   persisted.Connection,
			msgs:   ring,
			events: append([]LifecycleEvent(nil), persisted.Events...),
		}
	}

	return nil
}

// Reset clears all in-memory connection state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.connections = make(map[string]*connState)
	b.stats = Stats{}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
