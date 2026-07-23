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
	Tables              map[string]json.RawMessage                      `json:"tables"`
	Events              map[string][]StackEvent                         `json:"events"`
	Resources           map[string]map[string]*StackResource            `json:"resources"`
	ChangeSets          map[string]map[string]*ChangeSet                `json:"changeSets"`
	StackPolicies       map[string]string                               `json:"stackPolicies"`
	StackInstances      map[string][]StackInstance                      `json:"stackInstances"`
	StackSetOperations  map[string]map[string]*StackSetOperation        `json:"stackSetOperations"`
	TypeConfigs         map[string]string                               `json:"typeConfigs"`
	HandlerProgress     map[string]string                               `json:"handlerProgress"`
	Signals             map[string][]SignalRecord                       `json:"signals"`
	StackSetOpResults   map[string]map[string][]StackSetOperationResult `json:"stackSetOpResults"`
	TypeVersions        map[string][]*RegisteredTypeVersion             `json:"typeVersions"`
	ResourceScanItems   map[string][]ScannedResource                    `json:"resourceScanItems"`
	ResourceDriftStatus map[string]map[string]string                    `json:"resourceDriftStatus"`
	ResourceDriftDetail map[string]map[string]StackResourceDrift        `json:"resourceDriftDetail"`
	AccountID           string                                          `json:"accountID"`
	Region              string                                          `json:"region"`
	Version             int                                             `json:"version"`
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
		Version:             cfnSnapshotVersion,
		Tables:              tables,
		Events:              b.events,
		Resources:           b.resources,
		ChangeSets:          b.changeSets,
		StackPolicies:       b.stackPolicies,
		StackInstances:      b.stackInstances,
		StackSetOperations:  b.stackSetOperations,
		TypeConfigs:         b.typeConfigs,
		HandlerProgress:     b.handlerProgress,
		Signals:             b.signals,
		StackSetOpResults:   b.stackSetOpResults,
		TypeVersions:        b.typeVersions,
		ResourceScanItems:   b.resourceScanItems,
		ResourceDriftStatus: b.resourceDriftStatus,
		ResourceDriftDetail: b.resourceDriftDetail,
		AccountID:           b.accountID,
		Region:              b.region,
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

	snap.applyNilDefaults()
	b.assignSnapshotFields(snap)
	b.rebuildDerivedIndexes()

	return nil
}

// applyNilDefaults replaces every nil map/slice field on snap with an empty
// instance, so a snapshot written before one of these fields existed (or a
// hand-crafted/partial one) restores to empty collections rather than nil
// maps that panic on write. Split out of Restore to keep its cyclomatic
// complexity down -- this is a flat sequence of independent nil-checks, not
// branching logic.
func (snap *backendSnapshot) applyNilDefaults() {
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
	if snap.StackInstances == nil {
		snap.StackInstances = make(map[string][]StackInstance)
	}
	if snap.StackSetOperations == nil {
		snap.StackSetOperations = make(map[string]map[string]*StackSetOperation)
	}
	if snap.TypeConfigs == nil {
		snap.TypeConfigs = make(map[string]string)
	}
	if snap.HandlerProgress == nil {
		snap.HandlerProgress = make(map[string]string)
	}
	if snap.Signals == nil {
		snap.Signals = make(map[string][]SignalRecord)
	}
	if snap.StackSetOpResults == nil {
		snap.StackSetOpResults = make(map[string]map[string][]StackSetOperationResult)
	}
	if snap.TypeVersions == nil {
		snap.TypeVersions = make(map[string][]*RegisteredTypeVersion)
	}
	if snap.ResourceScanItems == nil {
		snap.ResourceScanItems = make(map[string][]ScannedResource)
	}
	if snap.ResourceDriftStatus == nil {
		snap.ResourceDriftStatus = make(map[string]map[string]string)
	}
	if snap.ResourceDriftDetail == nil {
		snap.ResourceDriftDetail = make(map[string]map[string]StackResourceDrift)
	}
}

// assignSnapshotFields copies every backendSnapshot field onto the backend.
// Caller must hold b.mu.Lock. Split out of Restore purely for readability --
// this is a flat, branch-free sequence of assignments.
func (b *InMemoryBackend) assignSnapshotFields(snap backendSnapshot) {
	b.events = snap.Events
	b.resources = snap.Resources
	b.changeSets = snap.ChangeSets
	b.stackPolicies = snap.StackPolicies
	b.stackInstances = snap.StackInstances
	b.stackSetOperations = snap.StackSetOperations
	b.typeConfigs = snap.TypeConfigs
	b.handlerProgress = snap.HandlerProgress
	b.signals = snap.Signals
	b.stackSetOpResults = snap.StackSetOpResults
	b.typeVersions = snap.TypeVersions
	b.resourceScanItems = snap.ResourceScanItems
	b.resourceDriftStatus = snap.ResourceDriftStatus
	b.resourceDriftDetail = snap.ResourceDriftDetail
	b.accountID = snap.AccountID
	b.region = snap.Region
}

// rebuildDerivedIndexes recomputes every index that is intentionally NOT
// persisted directly because it would be a second source of truth for data
// already covered by a persisted table (stackIDIndex from b.stacks,
// driftByStackID from b.driftDetections). Caller must hold b.mu.Lock.
func (b *InMemoryBackend) rebuildDerivedIndexes() {
	b.stackIDIndex = make(map[string]string, b.stacks.Len())
	for _, stack := range b.stacks.All() {
		b.stackIDIndex[stack.StackID] = stack.StackName
	}

	b.driftByStackID = make(map[string][]string)
	for _, dd := range b.driftDetections.All() {
		b.driftByStackID[dd.StackID] = append(b.driftByStackID[dd.StackID], dd.StackDriftDetectionID)
	}
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
