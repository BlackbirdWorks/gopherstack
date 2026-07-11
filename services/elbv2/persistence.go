package elbv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// errBackendNotInMemory is returned when the Handler's backend cannot be cast to *InMemoryBackend.
var errBackendNotInMemory = errors.New("elbv2: backend is not *InMemoryBackend")

// elbv2SnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (commit 12e611a4) and services/sqs (commit
// 0f09d77c) pilots.
const elbv2SnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the ELBv2 backend.
//
// Tables holds one JSON-encoded array per registered resource table, produced
// by [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll]
// -- "loadBalancers", "targetGroups", "listeners", "rules", and "trustStores".
// ResourcePolicies / TargetReadyAt / TargetDrainingUntil are persisted
// separately because they are not store.Table-shaped -- see store_setup.go.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage      `json:"tables"`
	ResourcePolicies    map[string]string               `json:"resourcePolicies"`
	TargetReadyAt       map[string]map[string]time.Time `json:"targetReadyAt"`
	TargetDrainingUntil map[string]map[string]time.Time `json:"targetDrainingUntil"`
	AccountID           string                          `json:"accountID"`
	Region              string                          `json:"region"`
	Version             int                             `json:"version"`
	RuleCounter         int                             `json:"ruleCounter"`
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
		logger.Load(ctx).WarnContext(ctx, "elbv2: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:             elbv2SnapshotVersion,
		Tables:              tables,
		ResourcePolicies:    b.resourcePolicies,
		TargetReadyAt:       b.targetReadyAt,
		TargetDrainingUntil: b.targetDrainingUntil,
		RuleCounter:         b.ruleCounter,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "elbv2", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elbv2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != elbv2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2 and services/sqs pilots.
		logger.Load(ctx).WarnContext(ctx,
			"elbv2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", elbv2SnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("elbv2: restore snapshot tables: %w", err)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]string)
	}

	// targetReadyAt / targetDrainingUntil are written to via `m[tgArn][key] = ...`,
	// which panics on a nil top-level map. Snapshots taken before these fields
	// existed (or taken while empty, which JSON-encodes to `{}` but can still
	// unmarshal as nil for an absent key) must not leave a nil map behind.
	if snap.TargetReadyAt == nil {
		snap.TargetReadyAt = make(map[string]map[string]time.Time)
	}

	if snap.TargetDrainingUntil == nil {
		snap.TargetDrainingUntil = make(map[string]map[string]time.Time)
	}

	b.resourcePolicies = snap.ResourcePolicies
	b.targetReadyAt = snap.TargetReadyAt
	b.targetDrainingUntil = snap.TargetDrainingUntil
	b.ruleCounter = snap.RuleCounter
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	return b.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return errBackendNotInMemory
	}

	return b.Restore(ctx, data)
}
