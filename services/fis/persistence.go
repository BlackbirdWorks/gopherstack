package fis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// fisSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (commit 12e611a4) and services/ses (commit
// e9af4cc7) conversions. The pre-Phase-3.3 snapshot format had no version
// field at all, so an old snapshot decodes with Version == 0, which is
// guaranteed to mismatch fisSnapshotVersion and is discarded the same way
// any other incompatible snapshot is.
const fisSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the FIS backend. Tables
// holds one JSON-encoded array per registered table name (templates,
// experiments, targetAccountConfigs -- see registerAllTables), produced by
// b.registry.SnapshotAll(). tplClientTokens and expClientTokens are
// intentionally NOT included: they were never part of the pre-Phase-3.3
// snapshot either, so idempotency tokens do not survive a snapshot/restore
// cycle -- this preserves that pre-existing behaviour unchanged.
type backendSnapshot struct {
	Tables      map[string]json.RawMessage `json:"tables"`
	SafetyLever *SafetyLever               `json:"safetyLever"`
	AccountID   string                     `json:"accountID"`
	Region      string                     `json:"region"`
	Version     int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "fis: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:     fisSnapshotVersion,
		Tables:      tables,
		SafetyLever: b.safetyLever,
		AccountID:   b.accountID,
		Region:      b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "fis: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It cancels any in-flight experiment goroutines before replacing state
// to prevent goroutine leaks when restored experiments have no cancel func.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "fis", data, &snap); err != nil {
		return err
	}

	// Cancel any running goroutines before replacing state.
	b.mu.Lock("Restore-cancel")

	for _, exp := range b.experiments.All() {
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	b.mu.Unlock()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != fisSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"fis: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", fisSnapshotVersion)

		b.registry.ResetAll()

		safetyLeverARN := arn.Build("fis", b.region, b.accountID, "safety-lever/"+b.accountID)
		b.safetyLever = &SafetyLever{
			ID:    b.accountID,
			Arn:   safetyLeverARN,
			Tags:  make(map[string]string),
			State: SafetyLeverState{Status: statusDisengaged},
		}

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("fis: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild or restore safety lever.
	if snap.SafetyLever != nil {
		b.safetyLever = snap.SafetyLever
	} else {
		safetyLeverARN := arn.Build("fis", b.region, b.accountID, "safety-lever/"+b.accountID)
		b.safetyLever = &SafetyLever{
			ID:    b.accountID,
			Arn:   safetyLeverARN,
			Tags:  make(map[string]string),
			State: SafetyLeverState{Status: statusDisengaged},
		}
	}

	markRestoredExperimentsTerminal(b.experiments.All())

	return nil
}

// markRestoredExperimentsTerminal marks any non-terminal experiment as failed.
// Restored experiments have no cancel func, so they cannot be resumed.
func markRestoredExperimentsTerminal(experiments []*Experiment) {
	now := time.Now()

	for _, exp := range experiments {
		if isActiveStatus(exp.Status.Status) {
			exp.Status = ExperimentStatus{Status: statusFailed, Reason: "restored from snapshot"}
			exp.EndTime = &now
		}
	}
}

// isActiveStatus returns true for non-terminal experiment statuses.
func isActiveStatus(s string) bool {
	switch s {
	case statusPending, statusInitiating, statusRunning, statusCompleting, statusStopping:
		return true
	}

	return false
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		return mem.Restore(ctx, data)
	}

	return nil
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		mem.Reset()
	}
}
