package awsconfig

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
// persistence audit below. Version 4 is not a field addition: it corrects nine
// wrong json tags on AggregationAuthorization, OrganizationConfigRule, and
// OrganizationConformancePack (models.go, commit 351ee095d) so responses match
// configservice@v1.68.4 on the wire. Those same tags are the on-disk key
// names for these store.Table-backed types, so a v3 snapshot holds the old
// keys and would decode into the corrected structs as empty strings --
// silent data loss, not a compatible extension.
// Version 5 is not a field addition either: gopherstack-ltj0d took
// remediationExecutions off b.registry (RuleName is a hidden json:"-")
// identity field, a component of remediationExecutionKeyFn/
// remediationExecutionRuleIndexKeyFn, that Registry.SnapshotAll silently
// dropped) and round-trips it through remediationExecutionSnapshot instead.
// This is purely additive to the on-disk shape (the new DTO adds a
// "ruleName" field an old snapshot lacks, decoding as "" -- the same lossy
// behavior the bug already produced, not new corruption), so the version
// constant does not change; see remediationExecutionSnapshot's doc comment
// below for the fix itself.
const awsconfigSnapshotVersion = 4

// remediationExecutionSnapshot is RemediationExecutionStatusEntry's persisted
// twin (gopherstack-ltj0d), the same wire/persisted conflation lambda's
// functionConfigurationSnapshot fixed (gopherstack-rluhj, commit
// dc4d95c5a). RuleName is a component of remediationExecutionKeyFn and
// remediationExecutionRuleIndexKeyFn (store_setup.go) but carries json:"-"
// on the live type: verified against the pinned SDK
// (aws-sdk-go-v2/service/configservice@v1.68.4 types/types.go:3187-3206)
// that real types.RemediationExecutionStatus has no rule-name member --
// DescribeRemediationExecutionStatus scopes its results by the required
// ConfigRuleName request parameter instead
// (api_op_DescribeRemediationExecutionStatus.go:34-37) -- so that tag is
// correct for the wire and stays.
//
// b.remediationExecutions was registered directly on b.registry with no DTO,
// so SnapshotAll honored the tag and dropped RuleName from persistence too:
// every entry decoded with RuleName == "", re-keying the whole table (and its
// "byRule" index) under the empty string. Two entries from different rules
// on the same resource key then collided under the same primary key
// ("|<resourceType>\x1f<resourceID>") and silently overwrote each other, and
// DescribeRemediationExecutionStatus/the cascade-delete at
// remediation.go:122-123 could no longer find anything under any real rule
// name.
//
// Embedding RemediationExecutionStatusEntry and redeclaring RuleName at depth
// 0 shadows the embedded json:"-" copy for Go field access (v.RuleName
// unambiguously means the outer field, since Go embedding always prefers the
// shallower depth) -- so every other field flows through unmodified without
// hand duplication.
type remediationExecutionSnapshot struct {
	RuleName string `json:"ruleName"`
	RemediationExecutionStatusEntry
}

// remediationExecutionSnapshotKey mirrors remediationExecutionKeyFn
// (store_setup.go) exactly, so the DTO table's Snapshot/Restore build is
// keyed identically to the live b.remediationExecutions table.
func remediationExecutionSnapshotKey(v *remediationExecutionSnapshot) string {
	return remediationExecutionKey(v.RuleName, v.ResourceKey.ResourceType, v.ResourceKey.ResourceID)
}

func toRemediationExecutionSnapshot(e *RemediationExecutionStatusEntry) *remediationExecutionSnapshot {
	return &remediationExecutionSnapshot{
		RuleName:                        e.RuleName,
		RemediationExecutionStatusEntry: *e,
	}
}

func fromRemediationExecutionSnapshot(v *remediationExecutionSnapshot) *RemediationExecutionStatusEntry {
	e := v.RemediationExecutionStatusEntry
	e.RuleName = v.RuleName

	return &e
}

// backendSnapshot is the top-level on-disk shape for the AWS Config backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry (see
// store_setup.go's registerAllTables): recorders, serviceLinkedRecorders,
// channels, deliveryStatus, connectors, aggregationAuths, configRules, aggregators,
// conformancePacks, conformancePackRules, orgConfigRules, orgConformancePacks,
// storedQueries, retentionConfigs, remediationConfigs, resourceEvaluations,
// resourceConfigs, and ruleResourceEvals, PLUS a "remediationExecutions"
// entry built separately from remediationExecutionSnapshot DTOs (see
// Snapshot/Restore below) -- b.remediationExecutions itself is not
// registered on b.registry (gopherstack-ltj0d, see store_setup.go's package
// doc and remediationExecutionSnapshot above). Most of the registered tables
// were already persisted pre-Phase-3.3 (as individual named map fields) or
// became persisted as a natural consequence of the store.Table conversion;
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

	// b.remediationExecutions is not on b.registry (gopherstack-ltj0d, see
	// store_setup.go's package doc and remediationExecutionSnapshot above),
	// so it is snapshotted separately through its own DTO registry.
	remDTOReg := store.NewRegistry()
	remDTOs := store.Register(remDTOReg, "remediationExecutions", store.New(remediationExecutionSnapshotKey))

	for _, e := range b.remediationExecutions.Snapshot() {
		remDTOs.Put(toRemediationExecutionSnapshot(e))
	}

	remTables, err := remDTOReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "awsconfig: snapshot remediationExecutions marshal failed", "error", err)

		return nil
	}

	tables["remediationExecutions"] = remTables["remediationExecutions"]

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
		b.remediationExecutions.Reset()
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

	if err := b.restoreRemediationExecutionsFromDTO(snap.Tables); err != nil {
		return err
	}

	return nil
}

// restoreRemediationExecutionsFromDTO restores b.remediationExecutions from
// its "remediationExecutions" DTO entry in tables (gopherstack-ltj0d;
// b.remediationExecutions is not on b.registry, see store_setup.go's package
// doc and remediationExecutionSnapshot above).
func (b *InMemoryBackend) restoreRemediationExecutionsFromDTO(tables map[string]json.RawMessage) error {
	remDTOReg := store.NewRegistry()
	remDTOs := store.Register(remDTOReg, "remediationExecutions", store.New(remediationExecutionSnapshotKey))

	if err := remDTOReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("awsconfig: restore snapshot remediationExecutions: %w", err)
	}

	liveExecutions := make([]*RemediationExecutionStatusEntry, 0, remDTOs.Len())
	for _, v := range remDTOs.All() {
		liveExecutions = append(liveExecutions, fromRemediationExecutionSnapshot(v))
	}

	b.remediationExecutions.Restore(liveExecutions)

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
