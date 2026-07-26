package awsconfig

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// awsconfigSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a registered table's value type or to
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. Version 1 was the first version with a version guard (the
// pre-Phase-3.3 backendSnapshot had no Version field at all, so any snapshot
// written before that change, including one with no version field which
// decodes as 0, was discarded the same way any other incompatible snapshot
// is). Version 2 added three tables introduced by the 2026-07 parity pass:
// conformancePackRules, remediationExecutions, and serviceLinkedRecorders.
// Version 3 adds the "connectors" table and the recorders table's new
// "byServicePrincipal" secondary index, both introduced by the SDK-bump pass
// that implemented PutConnector/GetConnector/ListConnectors/DeleteConnector
// and PutThirdPartyServiceLinkedConfigurationRecorder -- see this package's
// persistence audit below.
const awsconfigSnapshotVersion = 3

// backendSnapshot is the top-level on-disk shape for the AWS Config backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry (see
// store_setup.go's registerAllTables): recorders, serviceLinkedRecorders,
// channels, connectors, aggregationAuths, configRules, aggregators,
// conformancePacks, conformancePackRules, orgConfigRules, orgConformancePacks,
// storedQueries, retentionConfigs, remediationConfigs, remediationExecutions,
// resourceEvaluations, resourceConfigs, and ruleResourceEvals. Most were
// already persisted pre-Phase-3.3 (as individual named map fields) or became
// persisted as a natural consequence of the store.Table conversion;
// conformancePackRules/remediationExecutions/serviceLinkedRecorders were added
// by the 2026-07 parity pass, and connectors (plus the recorders table's new
// "byServicePrincipal" index, which rides the same table and needs no
// separate Tables entry) by the SDK-bump pass that added the connector family
// (see this package's persistence audit below).
//
// ruleEvaluations, resourceHistory, resourceTags, remediationExceptions,
// customRulePolicies, and orgCustomRulePolicies are deliberately NOT included:
// each is a scalar- or slice-valued map with no store.Table identity (see the
// field comments on InMemoryBackend in store.go), and none of the six was
// persisted before Phase 3.3 either -- this preserves that pre-existing gap
// rather than closing it, per the mechanical-conversion (not parity-fix)
// scope of that rollout.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "awsconfig: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version: awsconfigSnapshotVersion,
		Tables:  tables,
	}

	return persistence.MarshalSnapshot(ctx, "awsconfig", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "awsconfig", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != awsconfigSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"awsconfig: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", awsconfigSnapshotVersion)

		b.registry.ResetAll()
		b.ruleEvaluations = make(map[string]string)
		b.resourceHistory = make(map[string][]ResourceConfigItem)
		b.resourceTags = make(map[string][]Tag)
		b.remediationExceptions = make(map[string][]RemediationException)
		b.customRulePolicies = make(map[string]string)
		b.orgCustomRulePolicies = make(map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("awsconfig: restore snapshot tables: %w", err)
	}

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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
