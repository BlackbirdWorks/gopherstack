package mq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// mqSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// mqSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const mqSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Amazon MQ backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- both store.Table-backed resource fields are
// "clean" tables (see store_setup.go's file doc comment), so no ephemeral
// DTO registry is needed here. Tags is left as a plain map (not
// store.Table-backed, see store_setup.go) but was persisted before this
// refactor and remains so.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mq: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   mqSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mq: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "mq", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mqSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mq: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mqSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("mq: restore snapshot tables: %w", err)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	fixNilResourceMaps(b)
	reestablishTagPointers(b)

	return nil
}

// fixNilResourceMaps initialises nil maps on restored resources to empty
// maps, matching the pre-Phase-3.3 behavior of ensureNonNilMaps/
// fixNilTagMaps. Broker.Tags/Users and Configuration.Tags/Data all carry
// json:"-" tags, so json.Unmarshal never populates them -- they are always
// nil immediately after store.Table.Restore and must be defaulted here
// exactly as they were before this refactor.
func fixNilResourceMaps(b *InMemoryBackend) {
	for _, br := range b.brokers.All() {
		if br.Tags == nil {
			br.Tags = make(map[string]string)
		}

		if br.Users == nil {
			br.Users = make(map[string]*User)
		}
	}

	for _, cfg := range b.configurations.All() {
		if cfg.Tags == nil {
			cfg.Tags = make(map[string]string)
		}

		if cfg.Data == nil {
			cfg.Data = make(map[int32]string)
		}
	}

	for arnKey, tagMap := range b.tags {
		if tagMap == nil {
			b.tags[arnKey] = make(map[string]string)
		}
	}
}

// reestablishTagPointers re-establishes the shared-pointer invariant: b.tags[arn]
// and resource.Tags must point to the same map so that CreateTags and
// DeleteTags stay consistent without an extra linear scan.
func reestablishTagPointers(b *InMemoryBackend) {
	for _, br := range b.brokers.All() {
		if t, ok := b.tags[br.BrokerArn]; ok {
			br.Tags = t
		} else {
			b.tags[br.BrokerArn] = br.Tags
		}
	}

	for _, cfg := range b.configurations.All() {
		if t, ok := b.tags[cfg.Arn]; ok {
			cfg.Tags = t
		} else {
			b.tags[cfg.Arn] = cfg.Tags
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
