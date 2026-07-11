package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backupSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or the shape of any table
// registered on b.registry) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// backupSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const backupSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Backup backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// [store.Registry.SnapshotAll] on b.registry -- vaults, plans, jobs,
// selections, frameworks, legalHolds, reportPlans, restoreAccessVaults,
// restoreTestingPlans, and restoreTestingSelections (see store_setup.go).
// The VOLATILE tables (recoveryPoints, copyJobs, vaultAccessPolicies,
// vaultLockConfigs, vaultNotifications, restoreJobs, reportJobs, scanJobs,
// tieringConfigs, protectedResources) were never part of a snapshot before
// this refactor and are deliberately not registered on b.registry, so they
// remain unpersisted here too -- see store_setup.go's file doc comment.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage `json:"tables"`
	MpaApprovals map[string]string          `json:"mpaApprovals,omitempty"`
	AccountID    string                     `json:"accountID"`
	Region       string                     `json:"region"`
	Version      int                        `json:"version"`
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
		logger.Load(ctx).WarnContext(ctx, "backup: snapshot table marshal failed", "error", err)

		return nil
	}

	mpa := make(map[string]string, len(b.mpaApprovals))
	maps.Copy(mpa, b.mpaApprovals)

	snap := backendSnapshot{
		Version:      backupSnapshotVersion,
		Tables:       tables,
		MpaApprovals: mpa,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	return persistence.MarshalSnapshot(ctx, "backup", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "backup", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != backupSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"backup: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", backupSnapshotVersion)

		b.registry.ResetAll()
		b.mpaApprovals = make(map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("backup: restore snapshot tables: %w", err)
	}

	if snap.MpaApprovals == nil {
		snap.MpaApprovals = make(map[string]string)
	}

	b.mpaApprovals = snap.MpaApprovals
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildARNIndexes()

	return nil
}

// rebuildARNIndexes reconstructs all O(1) lookup indexes from the current
// tables. Must be called with mu held.
func (b *InMemoryBackend) rebuildARNIndexes() {
	vaults := b.vaults.All()
	b.vaultARNIndex = make(map[string]string, len(vaults))

	for _, v := range vaults {
		b.vaultARNIndex[v.BackupVaultArn] = v.BackupVaultName
	}

	plans := b.plans.All()
	b.planARNIndex = make(map[string]string, len(plans))
	b.planIDIndex = make(map[string]string, len(plans))

	for _, p := range plans {
		b.planARNIndex[p.BackupPlanArn] = p.BackupPlanName
		b.planIDIndex[p.BackupPlanID] = p.BackupPlanName
	}

	frameworks := b.frameworks.All()
	b.frameworkARNIndex = make(map[string]string, len(frameworks))

	for _, f := range frameworks {
		b.frameworkARNIndex[f.FrameworkArn] = f.FrameworkName
	}

	reportPlans := b.reportPlans.All()
	b.reportPlanARNIndex = make(map[string]string, len(reportPlans))

	for _, rp := range reportPlans {
		b.reportPlanARNIndex[rp.ReportPlanArn] = rp.ReportPlanName
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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
