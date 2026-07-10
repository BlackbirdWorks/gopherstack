package apprunner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// apprunnerSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// apprunnerSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const apprunnerSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the App Runner backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. asgByName and obsByName are NOT persisted
// directly (they never were, even before this conversion -- the pre-3.3
// Restore rebuilt both from the primary AutoScalingConfigurations/
// ObservabilityConfigs maps, not from a separately-saved field); Restore
// below rebuilds them the same way, from the now-restored tables. CustomDomains
// and Tags are the grouping/value maps left un-converted (their values are a
// plain order-sensitive slice and a string map, not *T); both were persisted
// before this conversion (part of the pre-Phase-3.3 snapshot struct in
// backend.go) and stay that way here. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables        map[string]json.RawMessage       `json:"tables"`
	CustomDomains map[string][]*storedCustomDomain `json:"customDomains"`
	Tags          map[string]map[string]string     `json:"tags"`
	Version       int                              `json:"version"`
}

// Snapshot serializes current state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are all plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "apprunner: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:       apprunnerSnapshotVersion,
		Tables:        tables,
		CustomDomains: b.customDomains,
		Tags:          b.tags,
	}

	return persistence.MarshalSnapshot(ctx, "apprunner", &snap)
}

// Restore deserializes state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "apprunner", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != apprunnerSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"apprunner: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", apprunnerSnapshotVersion)

		b.registry.ResetAll()
		b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
		b.obsByName = make(map[string][]*storedObservabilityConfiguration)
		b.customDomains = make(map[string][]*storedCustomDomain)
		b.tags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("apprunner: restore snapshot tables: %w", err)
	}

	// asgByName/obsByName are derived, not persisted (see backendSnapshot's
	// doc comment) -- rebuild them from the just-restored tables the same way
	// the pre-Phase-3.3 Restore rebuilt them from the raw
	// AutoScalingConfigurations/ObservabilityConfigs maps.
	b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
	for _, cfg := range b.autoScalingConfigs.Snapshot() {
		b.asgByName[cfg.AutoScalingConfigurationName] = append(b.asgByName[cfg.AutoScalingConfigurationName], cfg)
	}

	b.obsByName = make(map[string][]*storedObservabilityConfiguration)
	for _, cfg := range b.observabilityConfigs.Snapshot() {
		b.obsByName[cfg.ObservabilityConfigurationName] = append(b.obsByName[cfg.ObservabilityConfigurationName], cfg)
	}

	if snap.CustomDomains == nil {
		snap.CustomDomains = make(map[string][]*storedCustomDomain)
	}
	b.customDomains = snap.CustomDomains

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
	b.tags = snap.Tags

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Prior to Phase 3.3, Handler had no Snapshot/Restore of its own even though
// InMemoryBackend implemented both (dead wiring: cli.go's setupPersistence
// type-asserts each registered service.Registerable -- here, *Handler --
// against a persistable{Snapshot,Restore} interface, and only registers it
// with the persistence.Manager if that assertion succeeds; since Handler
// never declared these methods, App Runner was silently never registered and
// never persisted). This delegation is what actually wires App Runner into
// the persistence Manager.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
// See the Snapshot doc comment above for why this delegation is new.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
