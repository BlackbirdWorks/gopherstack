package kinesis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// kinesisSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to the persisted Stream shape or backendSnapshot
// itself would make an older snapshot unsafe to decode as the current shape.
// Restore discards (rather than partially decodes) any mismatch -- see
// Restore below. This is version 1: the first snapshot format built on top of
// pkgs/store's Table/Registry (the prior format persisted a bare
// region-nested map with no version field at all, so it always decodes as
// Version 0 and is treated as incompatible).
const kinesisSnapshotVersion = 1

// backendSnapshot is the persisted form of the backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry --
// currently just "streams" ([]*Stream), produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Stream is persisted directly (no DTO) because every field is already
// JSON-round-trippable except the unexported mu, which json.Marshal skips
// automatically and [initializeStreamRuntime] rebuilds on restore.
//
// ResourcePolicies remains a plain nested map: its value type (a bare string)
// carries no identity of its own to hand a store.Table keyFn, so it is
// persisted the same way it always was.
type backendSnapshot struct {
	Tables                   map[string]json.RawMessage   `json:"tables"`
	ResourcePolicies         map[string]map[string]string `json:"resourcePolicies,omitempty"`
	AccountID                string                       `json:"accountID"`
	Region                   string                       `json:"region"`
	OnDemandStreamCountLimit int                          `json:"onDemandStreamCountLimit,omitempty"`
	Version                  int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Note: shard sequence number counters are now serialised via the NextSeq field.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesis: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                  kinesisSnapshotVersion,
		Tables:                   tables,
		ResourcePolicies:         b.resourcePolicies,
		AccountID:                b.accountID,
		Region:                   b.region,
		OnDemandStreamCountLimit: b.onDemandStreamCountLimit,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesis: snapshot serialization failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kinesis", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != kinesisSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields (e.g. the pre-store nested-map format this
		// version replaced). Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition, not
		// data corruption.
		logger.Load(ctx).WarnContext(ctx,
			"kinesis: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", kinesisSnapshotVersion)

		b.registry.ResetAll()
		b.resourcePolicies = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("kinesis: restore snapshot tables: %w", err)
	}

	for _, stream := range b.streams.All() {
		initializeStreamRuntime(stream, stream.Name)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.resourcePolicies = snap.ResourcePolicies

	if snap.OnDemandStreamCountLimit > 0 {
		b.onDemandStreamCountLimit = snap.OnDemandStreamCountLimit
	} else {
		b.onDemandStreamCountLimit = defaultOnDemandStreamCountLimit
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
