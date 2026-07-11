package apigatewaymanagementapi

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// apigatewaymanagementapiSnapshotVersion identifies the shape of
// [backendSnapshot]. It must be bumped whenever a change to backendSnapshot
// (or a value type it holds) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (reset, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// apigatewaymanagementapiSnapshotVersion and is discarded the same way any
// other incompatible snapshot is.
//
// This backend has no [pkgs/store.Table] to register: its only internal
// resource map, connections, is keyed by connState, and connState embeds a
// live `downstream chan []byte` handle wired to an in-flight WebSocket
// simulation. A live channel has no meaningful JSON representation and can't
// survive a process restart regardless, so connState can never go through
// store.Table snapshot cleanly -- it stays a raw map (persistence-audit: see
// backendSnapshot's Connections field doc). Only the genuinely serializable
// parts of each connection (its Connection metadata, buffered messages, and
// lifecycle events) are captured via the persistedConn DTO; the channel is
// intentionally dropped on Restore, matching real AWS semantics where a
// restart drops live WebSocket connections anyway.
const apigatewaymanagementapiSnapshotVersion = 1

type persistedConn struct {
	Connection *Connection      `json:"connection"`
	Messages   []PostedMessage  `json:"messages,omitempty"`
	Events     []LifecycleEvent `json:"events,omitempty"`
}

// backendSnapshot is the top-level on-disk shape for the API Gateway
// Management API backend.
//
// Connections was persisted before this Phase 3.3 pass and must stay
// persisted (persistence-audit: was y, still y); it is left as a raw map
// (rather than a [pkgs/store.Table]) because its value type, connState,
// embeds a live channel handle -- see apigatewaymanagementapiSnapshotVersion's
// doc comment.
type backendSnapshot struct {
	Connections map[string]persistedConn `json:"connections"`
	Stats       Stats                    `json:"stats"`
	Version     int                      `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:     apigatewaymanagementapiSnapshotVersion,
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

	if snap.Version != apigatewaymanagementapiSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"apigatewaymanagementapi: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", apigatewaymanagementapiSnapshotVersion)

		b.connections = make(map[string]*connState)
		b.stats = Stats{}

		return nil
	}

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
