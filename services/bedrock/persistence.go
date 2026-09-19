package bedrock

// Code in this file wires up snapshot/restore persistence for the Bedrock
// backend, completing Phase 3.3 of the datalayer refactor for this package:
// store_setup.go registers every resource collection as a *store.Table[T] on
// b.registry, and this file drives that registry through
// [github.com/blackbirdworks/gopherstack/pkgs/persistence.Persistable].
import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// bedrockSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a registered table's value type or to
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw map/counter, not a
// partial decode) any mismatch -- see Restore below. This is the first
// version: bedrock had no persistence at all before this file (neither
// Handler/AgentsHandler nor InMemoryBackend implemented Snapshot/Restore), so
// there is no legacy snapshot shape to be compatible with.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for f16063cd2: Flow.FlowID/FlowArn,
// FlowAlias.FlowAliasID/FlowAliasArn, FlowVersion.FlowID, and
// Prompt.PromptID/PromptArn (all registered tables' value types: flows,
// flowAliases, flowVersions:<id>, prompts) were retagged from
// flowId/flowArn/flowAliasId/flowAliasArn/promptId/promptArn to the real
// deserializer's flat id/arn. A Version-1 snapshot's old keys no longer
// match at all, so these identity fields silently decode empty -- and since
// flowsKeyFn/promptsKeyFn key their table on exactly these fields, every
// restored flow/prompt would collide on the same empty key, silently
// discarding all but one. Must be discarded like any other
// shape-incompatible snapshot.
//
// Bumped 2 -> 3: ModelInvocationLoggingConfiguration was retagged from the fabricated flat
// s3BucketName/loggingEnabled fields (no such fields exist on the real types.LoggingConfig) to
// the real cloudWatchConfig/s3Config/*DataDeliveryEnabled shape. A Version-2 snapshot's old keys
// no longer match any field on the new type, so a stored logging config would silently decode
// as fully empty instead of restoring its cloudWatchConfig/s3Config/delivery flags.
//
// Not bumped when the agent/flow/prompt domain was deleted (gopherstack-m2eiu):
// a version-3 snapshot still decodes, the dead fields are simply ignored.
// Bumping would discard users' guardrail and model state for nothing.
const bedrockSnapshotVersion = 3

// backendSnapshot is the top-level on-disk shape for the Bedrock backend.
//
// Tables holds one JSON-encoded, key-sorted array per *store.Table[V]
// registered on b.registry (see store_setup.go).
//
// The *ByName / *ByARN fields are secondary name/ARN -> ID lookup indexes;
// ARPAnnotations is a grouping map whose value carries no identity field of
// its own -- neither is a fit for store.Table's func(*V) string keying, so
// they are carried here verbatim (see store_setup.go's registerAllTables doc
// comment for the authoritative per-field list).
//
// GuardrailVersionCounters exists ONLY because Guardrail.versionCounter is an
// unexported field on an otherwise-converted store.Table value type:
// encoding/json silently drops unexported fields on every Marshal, so
// store.Table.snapshotJSON/restoreJSON (used by registry.SnapshotAll/
// RestoreAll) can never round-trip it. Persisting it out-of-band here and
// re-applying it onto the already-restored table entries in
// restoreHiddenFields is what keeps a restored backend byte-for-byte
// equivalent to the one that was snapshotted, instead of silently dropping
// that state (see .claude/memories/parity-principles.md's no-stub rule):
// losing versionCounter would restart a restored guardrail's version
// numbering from 1, which then COLLIDES with (and silently overwrites, via
// guardrailVersionsKeyFn's "guardrailID:version" key) any GuardrailVersion
// already restored under the version number a subsequent
// CreateGuardrailVersion call reissues -- a real data-corruption risk, not
// just a cosmetic gap.
type backendSnapshot struct {
	Tables                         map[string]json.RawMessage           `json:"tables"`
	LoggingConfig                  *ModelInvocationLoggingConfiguration `json:"loggingConfig,omitempty"`
	GuardrailsByName               map[string]string                    `json:"guardrailsByName"`
	GuardrailsByARN                map[string]string                    `json:"guardrailsByARN"`
	PMTsByName                     map[string]string                    `json:"pmtsByName"`
	ARPByName                      map[string]string                    `json:"arpByName"`
	CustomModelsByName             map[string]string                    `json:"customModelsByName"`
	CustomModelDeployByName        map[string]string                    `json:"customModelDeployByName"`
	EvaluationJobsByName           map[string]string                    `json:"evaluationJobsByName"`
	CustomizationJobsByName        map[string]string                    `json:"customizationJobsByName"`
	InferenceProfilesByName        map[string]string                    `json:"inferenceProfilesByName"`
	MarketplaceEndpointsByName     map[string]string                    `json:"marketplaceEndpointsByName"`
	PromptRoutersByName            map[string]string                    `json:"promptRoutersByName"`
	ARPVersionCountByPolicy        map[string]int                       `json:"arpVersionCountByPolicy"`
	ARPAnnotations                 map[string][]any                     `json:"arpAnnotations"`
	GuardrailVersionCounters       map[string]int                       `json:"guardrailVersionCounters"`
	AccountDataRetention           *AccountDataRetention                `json:"accountDataRetention,omitempty"`
	AccountID                      string                               `json:"accountID"`
	Region                         string                               `json:"region"`
	UseCaseFormData                []byte                               `json:"useCaseFormData,omitempty"`
	GuardrailCounter               int                                  `json:"guardrailCounter"`
	GuardrailVersionCounter        int                                  `json:"guardrailVersionCounter"`
	ProvisionedCounter             int                                  `json:"provisionedCounter"`
	EvaluationJobCounter           int                                  `json:"evaluationJobCounter"`
	ARPCounter                     int                                  `json:"arpCounter"`
	ARPWorkflowCounter             int                                  `json:"arpWorkflowCounter"`
	ARPTestCaseCounter             int                                  `json:"arpTestCaseCounter"`
	CustomModelCounter             int                                  `json:"customModelCounter"`
	CustomModelDeployCounter       int                                  `json:"customModelDeployCounter"`
	CustomizationJobCounter        int                                  `json:"customizationJobCounter"`
	CopyJobCounter                 int                                  `json:"copyJobCounter"`
	ImportJobCounter               int                                  `json:"importJobCounter"`
	InferenceProfileCounter        int                                  `json:"inferenceProfileCounter"`
	MarketplaceEndpointCounter     int                                  `json:"marketplaceEndpointCounter"`
	ModelInvocationJobCounter      int                                  `json:"modelInvocationJobCounter"`
	PromptRouterCounter            int                                  `json:"promptRouterCounter"`
	EnforcedGuardrailConfigCounter int                                  `json:"enforcedGuardrailConfigCounter"`
	AdvancedPromptOptJobCounter    int                                  `json:"advancedPromptOptJobCounter"`
	ResourcePolicyRevisionCounter  int                                  `json:"resourcePolicyRevisionCounter"`
	Version                        int                                  `json:"version"`
}

// collectHiddenFields gathers GuardrailVersionCounters (see backendSnapshot's
// doc comment for why it exists) by scanning the already-registered
// guardrails table. Only non-zero entries are recorded, keeping the common
// case (no guardrail versioned yet) cheap. Callers must hold at least
// b.mu.RLock.
func collectHiddenFields(b *InMemoryBackend) map[string]int {
	guardrailVersionCounters := make(map[string]int)

	for _, g := range b.guardrails.All() {
		if g.versionCounter != 0 {
			guardrailVersionCounters[g.GuardrailID] = g.versionCounter
		}
	}

	return guardrailVersionCounters
}

// restoreHiddenFields re-applies GuardrailVersionCounters onto the table
// entries registry.RestoreAll just restored. It must run AFTER
// registry.RestoreAll so the target entries already exist. Callers must hold
// b.mu.Lock.
func restoreHiddenFields(b *InMemoryBackend, snap *backendSnapshot) {
	for id, counter := range snap.GuardrailVersionCounters {
		if g, ok := b.guardrails.Get(id); ok {
			g.versionCounter = counter
		}
	}
}

// snapshotRawState builds the non-Tables portion of backendSnapshot from b's
// current fields. Callers must hold at least b.mu.RLock.
func snapshotRawState(b *InMemoryBackend) backendSnapshot {
	guardrailVersionCounters := collectHiddenFields(b)

	return backendSnapshot{
		LoggingConfig:                  b.loggingConfig,
		GuardrailsByName:               b.guardrailsByName,
		GuardrailsByARN:                b.guardrailsByARN,
		PMTsByName:                     b.pmtsByName,
		ARPByName:                      b.arpByName,
		CustomModelsByName:             b.customModelsByName,
		CustomModelDeployByName:        b.customModelDeployByName,
		EvaluationJobsByName:           b.evaluationJobsByName,
		CustomizationJobsByName:        b.customizationJobsByName,
		InferenceProfilesByName:        b.inferenceProfilesByName,
		MarketplaceEndpointsByName:     b.marketplaceEndpointsByName,
		PromptRoutersByName:            b.promptRoutersByName,
		ARPVersionCountByPolicy:        b.arpVersionCountByPolicy,
		ARPAnnotations:                 b.arpAnnotations,
		GuardrailVersionCounters:       guardrailVersionCounters,
		AccountDataRetention:           b.accountDataRetention,
		AccountID:                      b.accountID,
		Region:                         b.region,
		UseCaseFormData:                b.useCaseFormData,
		GuardrailCounter:               b.guardrailCounter,
		GuardrailVersionCounter:        b.guardrailVersionCounter,
		ProvisionedCounter:             b.provisionedCounter,
		EvaluationJobCounter:           b.evaluationJobCounter,
		ARPCounter:                     b.arpCounter,
		ARPWorkflowCounter:             b.arpWorkflowCounter,
		ARPTestCaseCounter:             b.arpTestCaseCounter,
		CustomModelCounter:             b.customModelCounter,
		CustomModelDeployCounter:       b.customModelDeployCounter,
		CustomizationJobCounter:        b.customizationJobCounter,
		CopyJobCounter:                 b.copyJobCounter,
		ImportJobCounter:               b.importJobCounter,
		InferenceProfileCounter:        b.inferenceProfileCounter,
		MarketplaceEndpointCounter:     b.marketplaceEndpointCounter,
		ModelInvocationJobCounter:      b.modelInvocationJobCounter,
		PromptRouterCounter:            b.promptRouterCounter,
		EnforcedGuardrailConfigCounter: b.enforcedGuardrailConfigCounter,
		AdvancedPromptOptJobCounter:    b.advancedPromptOptJobCounter,
		ResourcePolicyRevisionCounter:  b.resourcePolicyRevisionCounter,
	}
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "bedrock: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := snapshotRawState(b)
	snap.Version = bedrockSnapshotVersion
	snap.Tables = tables

	return persistence.MarshalSnapshot(ctx, "bedrock", snap)
}

// resetRawState resets every raw (non-store.Table) map, counter, and scalar
// field to its zero state. Callers must hold b.mu.Lock.
func resetRawState(b *InMemoryBackend) {
	b.arpVersionCountByPolicy = make(map[string]int)
	b.loggingConfig = nil
	b.guardrailsByName = make(map[string]string)
	b.guardrailsByARN = make(map[string]string)
	b.pmtsByName = make(map[string]string)
	b.arpByName = make(map[string]string)
	b.customModelsByName = make(map[string]string)
	b.customModelDeployByName = make(map[string]string)
	b.evaluationJobsByName = make(map[string]string)
	b.customizationJobsByName = make(map[string]string)
	b.inferenceProfilesByName = make(map[string]string)
	b.marketplaceEndpointsByName = make(map[string]string)
	b.arpAnnotations = make(map[string][]any)
	// arpAnnotationSetHash/arpAnnotationsUpdatedAt intentionally have no
	// snapshot counterpart -- see their field doc comments in store.go.
	b.arpAnnotationSetHash = make(map[string]string)
	b.arpAnnotationsUpdatedAt = make(map[string]time.Time)
	b.promptRoutersByName = make(map[string]string)
	b.useCaseFormData = nil
	b.accountDataRetention = nil

	resetRawCounters(b)
}

// resetRawCounters resets every ID counter to zero. Split out of
// resetRawState to keep each function comfortably under the project's
// function-length gate. Callers must hold b.mu.Lock.
func resetRawCounters(b *InMemoryBackend) {
	b.guardrailCounter = 0
	b.guardrailVersionCounter = 0
	b.provisionedCounter = 0
	b.evaluationJobCounter = 0
	b.arpCounter = 0
	b.arpWorkflowCounter = 0
	b.arpTestCaseCounter = 0
	b.customModelCounter = 0
	b.customModelDeployCounter = 0
	b.customizationJobCounter = 0
	b.copyJobCounter = 0
	b.importJobCounter = 0
	b.inferenceProfileCounter = 0
	b.marketplaceEndpointCounter = 0
	b.modelInvocationJobCounter = 0
	b.promptRouterCounter = 0
	b.advancedPromptOptJobCounter = 0
	b.resourcePolicyRevisionCounter = 0
}

// restoreRawMaps restores every raw (non-store.Table) map from snap,
// defaulting each to empty when absent from the snapshot (e.g. a future
// snapshot taken by an older binary omitting a field bedrockSnapshotVersion's
// own guard would otherwise make unreachable today). Callers must hold
// b.mu.Lock.
func restoreRawMaps(b *InMemoryBackend, snap *backendSnapshot) {
	b.loggingConfig = snap.LoggingConfig
	b.guardrailsByName = nonNilStringMap(snap.GuardrailsByName)
	b.guardrailsByARN = nonNilStringMap(snap.GuardrailsByARN)
	b.pmtsByName = nonNilStringMap(snap.PMTsByName)
	b.arpByName = nonNilStringMap(snap.ARPByName)
	b.customModelsByName = nonNilStringMap(snap.CustomModelsByName)
	b.customModelDeployByName = nonNilStringMap(snap.CustomModelDeployByName)
	b.evaluationJobsByName = nonNilStringMap(snap.EvaluationJobsByName)
	b.customizationJobsByName = nonNilStringMap(snap.CustomizationJobsByName)
	b.inferenceProfilesByName = nonNilStringMap(snap.InferenceProfilesByName)
	b.marketplaceEndpointsByName = nonNilStringMap(snap.MarketplaceEndpointsByName)
	b.promptRoutersByName = nonNilStringMap(snap.PromptRoutersByName)

	b.arpVersionCountByPolicy = nonNilIntMap(snap.ARPVersionCountByPolicy)

	b.arpAnnotations = snap.ARPAnnotations
	if b.arpAnnotations == nil {
		b.arpAnnotations = make(map[string][]any)
	}

	b.accountDataRetention = snap.AccountDataRetention
}

// nonNilStringMap returns m unchanged if non-nil, otherwise a fresh empty map.
func nonNilStringMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return m
}

// nonNilIntMap returns m unchanged if non-nil, otherwise a fresh empty map.
func nonNilIntMap(m map[string]int) map[string]int {
	if m == nil {
		return make(map[string]int)
	}

	return m
}

// restoreRawCounters restores every ID counter from snap. Callers must hold
// b.mu.Lock.
func restoreRawCounters(b *InMemoryBackend, snap *backendSnapshot) {
	b.guardrailCounter = snap.GuardrailCounter
	b.guardrailVersionCounter = snap.GuardrailVersionCounter
	b.provisionedCounter = snap.ProvisionedCounter
	b.evaluationJobCounter = snap.EvaluationJobCounter
	b.arpCounter = snap.ARPCounter
	b.arpWorkflowCounter = snap.ARPWorkflowCounter
	b.arpTestCaseCounter = snap.ARPTestCaseCounter
	b.customModelCounter = snap.CustomModelCounter
	b.customModelDeployCounter = snap.CustomModelDeployCounter
	b.customizationJobCounter = snap.CustomizationJobCounter
	b.copyJobCounter = snap.CopyJobCounter
	b.importJobCounter = snap.ImportJobCounter
	b.inferenceProfileCounter = snap.InferenceProfileCounter
	b.marketplaceEndpointCounter = snap.MarketplaceEndpointCounter
	b.modelInvocationJobCounter = snap.ModelInvocationJobCounter
	b.promptRouterCounter = snap.PromptRouterCounter
	b.enforcedGuardrailConfigCounter = snap.EnforcedGuardrailConfigCounter
	b.advancedPromptOptJobCounter = snap.AdvancedPromptOptJobCounter
	b.resourcePolicyRevisionCounter = snap.ResourcePolicyRevisionCounter
}

// restoreRawState restores every raw (non-store.Table) map, counter, and
// scalar field from snap, plus AccountID/Region/UseCaseFormData. Callers must
// hold b.mu.Lock.
func restoreRawState(b *InMemoryBackend, snap *backendSnapshot) {
	restoreRawMaps(b, snap)
	restoreRawCounters(b, snap)

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.useCaseFormData = snap.UseCaseFormData
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "bedrock", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != bedrockSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors services/ssm, services/appsync, and
		// services/bedrockagent.
		logger.Load(ctx).WarnContext(ctx,
			"bedrock: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", bedrockSnapshotVersion)

		b.registry.ResetAll()
		resetRawState(b)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("bedrock: restore snapshot tables: %w", err)
	}

	restoreHiddenFields(b, &snap)
	restoreRawState(b, &snap)

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
