package vpclattice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// vpclatticeSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// vpclatticeSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const vpclatticeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the VPC Lattice
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. Targets, AuthPolicies, ResourcePolicies, and Tags
// are the grouping/value maps left un-converted (their values are plain
// slices/strings/string maps, not *T); all four were persisted before this
// conversion (part of the pre-Phase-3.3 snapshot struct in backend.go) and
// stay that way here. Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage   `json:"tables"`
	Targets          map[string][]*storedTarget   `json:"targets"`
	AuthPolicies     map[string]string            `json:"authPolicies"`
	ResourcePolicies map[string]string            `json:"resourcePolicies"`
	Tags             map[string]map[string]string `json:"tags"`
	AccountID        string                       `json:"accountID"`
	Region           string                       `json:"region"`
	Version          int                          `json:"version"`
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
		logger.Load(ctx).WarnContext(ctx, "vpclattice: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:          vpclatticeSnapshotVersion,
		Tables:           tables,
		Targets:          b.targets,
		AuthPolicies:     b.authPolicies,
		ResourcePolicies: b.resourcePolicies,
		Tags:             b.tags,
		AccountID:        b.accountID,
		Region:           b.region,
	}

	return persistence.MarshalSnapshot(ctx, "vpclattice", &snap)
}

// Restore deserializes state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "vpclattice", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != vpclatticeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"vpclattice: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", vpclatticeSnapshotVersion)

		b.registry.ResetAll()
		b.targets = make(map[string][]*storedTarget)
		b.authPolicies = make(map[string]string)
		b.resourcePolicies = make(map[string]string)
		b.tags = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("vpclattice: restore snapshot tables: %w", err)
	}

	if snap.Targets == nil {
		snap.Targets = make(map[string][]*storedTarget)
	}

	if snap.AuthPolicies == nil {
		snap.AuthPolicies = make(map[string]string)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]string)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	b.targets = snap.Targets
	b.authPolicies = snap.AuthPolicies
	b.resourcePolicies = snap.ResourcePolicies
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Prior to Phase 3.3, Handler had no Snapshot/Restore of its own even though
// InMemoryBackend implemented both (dead wiring: cli.go's setupPersistence
// type-asserts each registered service.Registerable -- here, *Handler --
// against a persistable{Snapshot,Restore} interface, and only registers it
// with the persistence.Manager if that assertion succeeds; since Handler
// never declared these methods, VPC Lattice was silently never registered
// and never persisted). This delegation is what actually wires VPC Lattice
// into the persistence Manager.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
// See the Snapshot doc comment above for why this delegation is new.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
