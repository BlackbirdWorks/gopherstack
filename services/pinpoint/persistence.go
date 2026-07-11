package pinpoint

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// pinpointSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to the persisted table set (or a persisted
// value type) would make an older snapshot unsafe to decode as the current
// shape. Restore compares this against the persisted value and discards
// (rather than attempts to partially decode) any mismatch — see Restore
// below.
const pinpointSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Pinpoint backend.
//
// Tables holds one JSON-encoded array per persisted table, produced by
// [store.Registry.SnapshotAll]. Only the resource kinds Pinpoint has always
// persisted are included (apps, campaigns, emailTemplates, exportJobs,
// importJobs, inAppTemplates, journeys, pushTemplates, recommenders,
// segments, smsTemplates) — voiceTemplates, endpoints, eventStreams, and
// channels were never part of a persisted snapshot before this refactor and
// remain excluded so behaviour is preserved exactly.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// persistRegistry builds an ephemeral [store.Registry] over exactly the live
// tables this backend has always persisted, registering the SAME *store.Table
// pointers the backend itself reads and writes through (rather than copying
// into a separate DTO type), since every persisted value type here is
// already a plain JSON-friendly struct with no live/non-serialisable state.
// It is rebuilt on every Snapshot/Restore call rather than cached as a
// long-lived field: [store.Register] panics on a duplicate name, and
// registering the same tables into two different [store.Registry] values is
// safe (Registry holds no back-reference on the Table), so a fresh registry
// scoped to the call is simpler than guarding a shared one.
func (b *InMemoryBackend) persistRegistry() *store.Registry {
	reg := store.NewRegistry()
	store.Register(reg, "apps", b.apps)
	store.Register(reg, "campaigns", b.campaigns)
	store.Register(reg, "emailTemplates", b.emailTemplates)
	store.Register(reg, "exportJobs", b.exportJobs)
	store.Register(reg, "importJobs", b.importJobs)
	store.Register(reg, "inAppTemplates", b.inAppTemplates)
	store.Register(reg, "journeys", b.journeys)
	store.Register(reg, "pushTemplates", b.pushTemplates)
	store.Register(reg, "recommenders", b.recommenders)
	store.Register(reg, "segments", b.segments)
	store.Register(reg, "smsTemplates", b.smsTemplates)

	return reg
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.persistRegistry().SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pinpoint: failed to marshal snapshot", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   pinpointSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "pinpoint", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "pinpoint", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != pinpointSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"pinpoint: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", pinpointSnapshotVersion)

		b.registry.ResetAll()
		b.arnIndex = make(map[string]tagHolder)

		return nil
	}

	if err := b.persistRegistry().RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("pinpoint: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from all restored resources.
	b.arnIndex = make(map[string]tagHolder)
	rebuildARNIndexLocked(b)

	return nil
}

// rebuildARNIndexLocked rebuilds arnIndex from all in-memory resources.
// Must be called with b.mu write lock held.
func rebuildARNIndexLocked(b *InMemoryBackend) {
	for _, v := range b.apps.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.campaigns.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.emailTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.inAppTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.journeys.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.pushTemplates.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.segments.All() {
		b.arnIndex[v.ARN] = v
	}

	for _, v := range b.smsTemplates.All() {
		b.arnIndex[v.ARN] = v
	}
}
