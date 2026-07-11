package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// cfnSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/sqs pilot (commit 0f09d77c) and the services/ec2
// conversion (commit 12e611a4).
const cfnSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the CloudFormation
// backend.
//
// Tables holds one JSON-encoded array per registered store.Table (see
// registerAllTables in store_setup.go), produced by
// [store.Registry.SnapshotAll]. The remaining fields cover the maps that
// were NOT converted to store.Table (nested or one-to-many; see the doc
// comment on registerAllTables) and are persisted exactly as before the
// conversion.
type backendSnapshot struct {
	Tables        map[string]json.RawMessage           `json:"tables"`
	Events        map[string][]StackEvent              `json:"events"`
	Resources     map[string]map[string]*StackResource `json:"resources"`
	ChangeSets    map[string]map[string]*ChangeSet     `json:"changeSets"`
	StackPolicies map[string]string                    `json:"stackPolicies"`
	AccountID     string                               `json:"accountID"`
	Region        string                               `json:"region"`
	Version       int                                  `json:"version"`
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
		logger.Load(ctx).WarnContext(ctx, "cloudformation: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:       cfnSnapshotVersion,
		Tables:        tables,
		Events:        b.events,
		Resources:     b.resources,
		ChangeSets:    b.changeSets,
		StackPolicies: b.stackPolicies,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cloudformation", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cloudformation", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cfnSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c).
		logger.Load(ctx).WarnContext(ctx,
			"cloudformation: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cfnSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudformation: restore snapshot tables: %w", err)
	}

	if snap.Events == nil {
		snap.Events = make(map[string][]StackEvent)
	}

	if snap.Resources == nil {
		snap.Resources = make(map[string]map[string]*StackResource)
	}

	if snap.ChangeSets == nil {
		snap.ChangeSets = make(map[string]map[string]*ChangeSet)
	}

	if snap.StackPolicies == nil {
		snap.StackPolicies = make(map[string]string)
	}

	b.events = snap.Events
	b.resources = snap.Resources
	b.changeSets = snap.ChangeSets
	b.stackPolicies = snap.StackPolicies
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the stackIDIndex from the restored stacks.
	b.stackIDIndex = make(map[string]string, b.stacks.Len())
	for _, stack := range b.stacks.All() {
		b.stackIDIndex[stack.StackID] = stack.StackName
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
