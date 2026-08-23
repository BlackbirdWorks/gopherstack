package appsync

// Code in this file wires up snapshot/restore persistence for the AppSync
// backend, completing Phase 3.3 of the datalayer refactor: store_setup.go
// already registers every "clean" resource collection on b.registry (see its
// doc comment), but until now nothing drove that registry through
// [github.com/blackbirdworks/gopherstack/pkgs/persistence.Persistable] --
// AppSync state did not survive a snapshot/restore cycle. This mirrors the
// services/ssm and services/apigateway conversions (both of which register
// every "clean" table directly, same as here).
import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// appsyncSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (the set of resource tables registered on b.registry -- see
// store_setup.go) plus the explicit APIKeys field below. It must be bumped
// whenever a change there would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. Mirrors the services/ssm and services/secretsmanager
// conversions.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for d83f4b5d3: Resolver (the registered
// "resolvers" table's value type) gained a custom MarshalJSON/UnmarshalJSON
// pair that renders PipelineConfig as the real deserializer's
// {functions: [...]} object instead of the previous bare array. A Version-1
// snapshot's array no longer unmarshals into the new object field at all --
// an outright decode error that takes down the whole restore, not silent
// loss -- so it must be discarded like any other shape-incompatible
// snapshot.
const appsyncSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the AppSync backend.
//
// Tables holds one JSON-encoded, key-sorted array per *store.Table[V]
// registered on b.registry, produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
//
// APIKeys is the one resource collection deliberately left as a plain
// nested map rather than a *store.Table -- see store_setup.go's package doc
// for why (APIKey carries no APIID field of its own, so there is no pure
// func(*APIKey) string that could reproduce the "<apiID>#<keyID>" composite
// key from the value alone). It is persisted explicitly here instead.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage    `json:"tables"`
	APIKeys map[string]map[string]*APIKey `json:"apiKeys,omitempty"`
	Version int                           `json:"version"`
}

// closeReplacedTags closes the Tags handle on every GraphqlAPI and DataSource
// currently in the backend before their state is discarded by Restore, to
// avoid leaking their lockmetrics.RWMutex registrations. GraphqlAPI and
// DataSource are the only two resource types carrying a *tags.Tags field
// (see models.go); mirrors InMemoryBackend.Reset's identical loop and the
// services/secretsmanager persistence.go precedent.
func closeReplacedTags(b *InMemoryBackend) {
	for _, api := range b.apis.All() {
		if api.Tags != nil {
			api.Tags.Close()
		}
	}

	for _, ds := range b.datasources.All() {
		if ds != nil && ds.Tags != nil {
			ds.Tags.Close()
		}
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "appsync: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version: appsyncSnapshotVersion,
		Tables:  tables,
		APIKeys: b.apiKeys,
	}

	return persistence.MarshalSnapshot(ctx, "appsync", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "appsync", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	closeReplacedTags(b)

	if snap.Version != appsyncSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ssm and services/apigateway
		// conversions.
		logger.Load(ctx).WarnContext(ctx,
			"appsync: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", appsyncSnapshotVersion)

		b.registry.ResetAll()
		b.apiKeys = make(map[string]map[string]*APIKey)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("appsync: restore snapshot tables: %w", err)
	}

	if snap.APIKeys == nil {
		snap.APIKeys = make(map[string]map[string]*APIKey)
	}

	b.apiKeys = snap.APIKeys

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
