package redshift

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// redshiftSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (i.e. the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 conversion (commit 12e611a4) and the services/sqs
// pilot (commit 0f09d77c).
const redshiftSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Redshift backend.
//
// Tables holds one JSON-encoded array per registered store.Table, produced by
// [store.Registry.SnapshotAll]. The remaining fields are the handful of
// resource maps that could not be registered (see store_setup.go for why:
// each is keyed externally by clusterID rather than by a field on the value
// itself), plus backend-level scalar configuration.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage     `json:"tables"`
	ActiveResizes       map[string]*ResizeProgress     `json:"activeResizes"`
	SnapshotCopyConfigs map[string]*SnapshotCopyConfig `json:"snapshotCopyConfigs"`
	LoggingStatuses     map[string]*LoggingStatus      `json:"loggingStatuses"`
	AccountID           string                         `json:"accountID"`
	Region              string                         `json:"region"`
	Version             int                            `json:"version"`
}

func (s *backendSnapshot) ensureNonNilMaps() {
	if s.ActiveResizes == nil {
		s.ActiveResizes = make(map[string]*ResizeProgress)
	}

	if s.SnapshotCopyConfigs == nil {
		s.SnapshotCopyConfigs = make(map[string]*SnapshotCopyConfig)
	}

	if s.LoggingStatuses == nil {
		s.LoggingStatuses = make(map[string]*LoggingStatus)
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
		logger.Load(ctx).WarnContext(ctx, "redshift: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:             redshiftSnapshotVersion,
		Tables:              tables,
		ActiveResizes:       b.activeResizes,
		SnapshotCopyConfigs: b.snapshotCopyConfigs,
		LoggingStatuses:     b.loggingStatuses,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "redshift: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "redshift", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNilMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != redshiftSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors services/ec2 (commit 12e611a4) and
		// services/sqs (commit 0f09d77c).
		logger.Load(ctx).WarnContext(ctx,
			"redshift: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", redshiftSnapshotVersion)

		b.registry.ResetAll()
		b.activeResizes = make(map[string]*ResizeProgress)
		b.snapshotCopyConfigs = make(map[string]*SnapshotCopyConfig)
		b.loggingStatuses = make(map[string]*LoggingStatus)
		b.clusterTransitions = make(map[string]*clusterTransition)
		b.resetServerlessIndexes()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("redshift: restore snapshot tables: %w", err)
	}

	b.activeResizes = snap.ActiveResizes
	b.snapshotCopyConfigs = snap.SnapshotCopyConfigs
	b.loggingStatuses = snap.LoggingStatuses
	b.accountID = snap.AccountID
	b.region = snap.Region

	// In-flight cluster transitions are not persisted; start clean and rebuild
	// the serverless sorted indexes from the freshly loaded tables.
	b.clusterTransitions = make(map[string]*clusterTransition)
	b.rebuildServerlessIndexes()

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
