package bedrock

// Code in this file wires up snapshot/restore persistence for the Bedrock
// backend, completing Phase 3.3 of the datalayer refactor for this package:
// store_setup.go already registers every "clean" resource collection (30
// flat tables plus the 4 lazily-registered per-parent kinds) on b.registry,
// but until now nothing drove that registry through
// [github.com/blackbirdworks/gopherstack/pkgs/persistence.Persistable] --
// bedrock state did not survive a snapshot/restore cycle at all (see
// store_setup.go's package doc). This mirrors the services/ssm,
// services/appsync, and (most closely, being the sibling API surface)
// services/bedrockagent conversions.
//
// Two distinct service.Registerable values share this backend type at
// runtime -- Handler (service name "Bedrock") and AgentsHandler (service
// name "BedrockAgents", see provider.go's Provider and AgentsProvider) --
// but provider.go constructs each with its OWN InMemoryBackend instance, so
// each gets its own independent Snapshot/Restore delegation at the bottom of
// this file; there is exactly one InMemoryBackend.Snapshot/Restore pair
// underneath both.
import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
const bedrockSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Bedrock backend.
//
// Tables holds one JSON-encoded, key-sorted array per *store.Table[V]
// registered on b.registry (see store_setup.go), including the 4 lazily
// registered per-parent kinds (flowVersions:<flowID>,
// promptVersions:<promptID>, agentVersions:<agentID>,
// agentCollaborators:<agentID>) -- Restore must pre-register every
// (kind, parentID) pair found in Tables before calling registry.RestoreAll,
// see preRegisterLazyTables below.
//
// The *ByName / *ByARN fields are secondary name/ARN -> ID lookup indexes;
// the version-counter maps are per-parent "next version number" counters;
// AgentTags, AgentMemory, and ARPAnnotations are grouping maps whose value
// carries no identity field of its own -- none of these are a fit for
// store.Table's func(*V) string keying, so they are carried here verbatim,
// same rationale as services/ssm and services/bedrockagent's un-converted
// fields (see store_setup.go's registerAllTables doc comment for the
// authoritative per-field list).
//
// GuardrailVersionCounters, AgentPreparationDueAt, and
// IngestionJobCompletionDueAt exist ONLY because Guardrail.versionCounter,
// Agent.preparationDueAt, and IngestionJob.completionDueAt are unexported
// fields on otherwise-converted store.Table value types: encoding/json
// silently drops unexported fields on every Marshal, so
// store.Table.snapshotJSON/restoreJSON (used by registry.SnapshotAll/
// RestoreAll) can never round-trip them. Persisting them out-of-band here and
// re-applying them onto the already-restored table entries in
// restoreHiddenFields is what keeps a restored backend byte-for-byte
// equivalent to the one that was snapshotted, instead of silently dropping
// that state (see .claude/memories/parity-principles.md's no-stub rule):
//   - GuardrailVersionCounters: losing versionCounter would restart a
//     restored guardrail's version numbering from 1, which then COLLIDES
//     with (and silently overwrites, via guardrailVersionsKeyFn's
//     "guardrailID:version" key) any GuardrailVersion already restored under
//     the version number a subsequent CreateGuardrailVersion call reissues --
//     a real data-corruption risk, not just a cosmetic gap.
//   - AgentPreparationDueAt / IngestionJobCompletionDueAt: these are
//     best-effort status-transition timers (PREPARING -> terminal state,
//     STARTING -> COMPLETE); losing one only makes a restored resource that
//     was mid-transition at snapshot time advance to its terminal status up
//     to agentTransitionDelay/ingestionCompleteDelay early on the next
//     read/janitor tick, rather than corrupting or dropping data. Persisted
//     anyway for full fidelity since the cost is trivial.
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
	AgentsByName                   map[string]string                    `json:"agentsByName"`
	KBByName                       map[string]string                    `json:"kbByName"`
	FlowsByName                    map[string]string                    `json:"flowsByName"`
	PromptsByName                  map[string]string                    `json:"promptsByName"`
	ARPVersionCountByPolicy        map[string]int                       `json:"arpVersionCountByPolicy"`
	FlowVersionCounters            map[string]int                       `json:"flowVersionCounters"`
	PromptVersionCounters          map[string]int                       `json:"promptVersionCounters"`
	AgentVersionCounters           map[string]int                       `json:"agentVersionCounters"`
	AgentTags                      map[string]map[string]string         `json:"agentTags"`
	AgentMemory                    map[string][]any                     `json:"agentMemory"`
	ARPAnnotations                 map[string][]any                     `json:"arpAnnotations"`
	GuardrailVersionCounters       map[string]int                       `json:"guardrailVersionCounters"`
	AgentPreparationDueAt          map[string]time.Time                 `json:"agentPreparationDueAt"`
	IngestionJobCompletionDueAt    map[string]time.Time                 `json:"ingestionJobCompletionDueAt"`
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
	AgentCounter                   int                                  `json:"agentCounter"`
	ActionGroupCounter             int                                  `json:"actionGroupCounter"`
	AgentAliasCounter              int                                  `json:"agentAliasCounter"`
	KBCounter                      int                                  `json:"kbCounter"`
	DataSourceCounter              int                                  `json:"dataSourceCounter"`
	IngestionJobCounter            int                                  `json:"ingestionJobCounter"`
	FlowCounter                    int                                  `json:"flowCounter"`
	FlowAliasCounter               int                                  `json:"flowAliasCounter"`
	PromptCounter                  int                                  `json:"promptCounter"`
	AgentCollabCounter             int                                  `json:"agentCollabCounter"`
	AdvancedPromptOptJobCounter    int                                  `json:"advancedPromptOptJobCounter"`
	ResourcePolicyRevisionCounter  int                                  `json:"resourcePolicyRevisionCounter"`
	Version                        int                                  `json:"version"`
}

// collectHiddenFields gathers GuardrailVersionCounters, AgentPreparationDueAt,
// and IngestionJobCompletionDueAt (see backendSnapshot's doc comment for why
// these three exist) by scanning the already-registered tables. Only
// non-zero entries are recorded, keeping the common case (no guardrail
// versioned yet, no agent mid-preparation, no ingestion job mid-run) cheap.
// Callers must hold at least b.mu.RLock.
func collectHiddenFields(
	b *InMemoryBackend,
) (map[string]int, map[string]time.Time, map[string]time.Time) {
	guardrailVersionCounters := make(map[string]int)

	for _, g := range b.guardrails.All() {
		if g.versionCounter != 0 {
			guardrailVersionCounters[g.GuardrailID] = g.versionCounter
		}
	}

	agentPreparationDueAt := make(map[string]time.Time)

	for _, a := range b.agents.All() {
		if !a.preparationDueAt.IsZero() {
			agentPreparationDueAt[a.AgentID] = a.preparationDueAt
		}
	}

	ingestionJobCompletionDueAt := make(map[string]time.Time)

	for _, j := range b.ingestionJobs.All() {
		if !j.completionDueAt.IsZero() {
			ingestionJobCompletionDueAt[ingestionJobsKeyFn(j)] = j.completionDueAt
		}
	}

	return guardrailVersionCounters, agentPreparationDueAt, ingestionJobCompletionDueAt
}

// restoreHiddenFields re-applies GuardrailVersionCounters,
// AgentPreparationDueAt, and IngestionJobCompletionDueAt onto the table
// entries registry.RestoreAll just restored. It must run AFTER
// registry.RestoreAll so the target entries already exist. Callers must hold
// b.mu.Lock.
func restoreHiddenFields(b *InMemoryBackend, snap *backendSnapshot) {
	for id, counter := range snap.GuardrailVersionCounters {
		if g, ok := b.guardrails.Get(id); ok {
			g.versionCounter = counter
		}
	}

	for id, dueAt := range snap.AgentPreparationDueAt {
		if a, ok := b.agents.Get(id); ok {
			a.preparationDueAt = dueAt
		}
	}

	for key, dueAt := range snap.IngestionJobCompletionDueAt {
		if j, ok := b.ingestionJobs.Get(key); ok {
			j.completionDueAt = dueAt
		}
	}
}

// snapshotRawState builds the non-Tables portion of backendSnapshot from b's
// current fields. Callers must hold at least b.mu.RLock.
func snapshotRawState(b *InMemoryBackend) backendSnapshot {
	guardrailVersionCounters, agentPreparationDueAt, ingestionJobCompletionDueAt := collectHiddenFields(b)

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
		AgentsByName:                   b.agentsByName,
		KBByName:                       b.kbByName,
		FlowsByName:                    b.flowsByName,
		PromptsByName:                  b.promptsByName,
		ARPVersionCountByPolicy:        b.arpVersionCountByPolicy,
		FlowVersionCounters:            b.flowVersionCounters,
		PromptVersionCounters:          b.promptVersionCounters,
		AgentVersionCounters:           b.agentVersionCounters,
		AgentTags:                      b.agentTags,
		AgentMemory:                    b.agentMemory,
		ARPAnnotations:                 b.arpAnnotations,
		GuardrailVersionCounters:       guardrailVersionCounters,
		AgentPreparationDueAt:          agentPreparationDueAt,
		IngestionJobCompletionDueAt:    ingestionJobCompletionDueAt,
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
		AgentCounter:                   b.agentCounter,
		ActionGroupCounter:             b.actionGroupCounter,
		AgentAliasCounter:              b.agentAliasCounter,
		KBCounter:                      b.kbCounter,
		DataSourceCounter:              b.dataSourceCounter,
		IngestionJobCounter:            b.ingestionJobCounter,
		FlowCounter:                    b.flowCounter,
		FlowAliasCounter:               b.flowAliasCounter,
		PromptCounter:                  b.promptCounter,
		AgentCollabCounter:             b.agentCollabCounter,
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

// preRegisterLazyTables ensures every (kind, parentID) pair present in tables
// is registered on b.registry before registry.RestoreAll is called. See
// store_setup.go's lazyTableAccessorsByPrefix doc comment for why this is
// necessary. Each lazy table's name has the shape "<kind>:<parentID>" (see
// flowVersionsStore et al.); flat table names never contain ":", so
// splitting on the first one safely no-ops for them.
func preRegisterLazyTables(b *InMemoryBackend, tables map[string]json.RawMessage) {
	for key := range tables {
		kind, parentID, found := strings.Cut(key, ":")
		if !found {
			continue
		}

		if register, ok := lazyTableAccessorsByPrefix[kind]; ok {
			register(b, parentID)
		}
	}
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
	b.agentsByName = make(map[string]string)
	b.kbByName = make(map[string]string)
	b.flowsByName = make(map[string]string)
	b.flowVersionCounters = make(map[string]int)
	b.promptsByName = make(map[string]string)
	b.promptVersionCounters = make(map[string]int)
	b.agentVersionCounters = make(map[string]int)
	b.agentTags = make(map[string]map[string]string)
	b.agentMemory = make(map[string][]any)
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
	b.agentCounter = 0
	b.actionGroupCounter = 0
	b.agentAliasCounter = 0
	b.kbCounter = 0
	b.dataSourceCounter = 0
	b.ingestionJobCounter = 0
	b.flowCounter = 0
	b.flowAliasCounter = 0
	b.promptCounter = 0
	b.agentCollabCounter = 0
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
	b.agentsByName = nonNilStringMap(snap.AgentsByName)
	b.kbByName = nonNilStringMap(snap.KBByName)
	b.flowsByName = nonNilStringMap(snap.FlowsByName)
	b.promptsByName = nonNilStringMap(snap.PromptsByName)

	restoreRawCounterMaps(b, snap)

	b.agentTags = snap.AgentTags
	if b.agentTags == nil {
		b.agentTags = make(map[string]map[string]string)
	}

	b.agentMemory = snap.AgentMemory
	if b.agentMemory == nil {
		b.agentMemory = make(map[string][]any)
	}

	b.arpAnnotations = snap.ARPAnnotations
	if b.arpAnnotations == nil {
		b.arpAnnotations = make(map[string][]any)
	}

	b.accountDataRetention = snap.AccountDataRetention
}

// restoreRawCounterMaps restores the per-parent map[string]int version
// counters. Split out of restoreRawMaps to keep each function comfortably
// under the project's function-length gate. Callers must hold b.mu.Lock.
func restoreRawCounterMaps(b *InMemoryBackend, snap *backendSnapshot) {
	b.arpVersionCountByPolicy = nonNilIntMap(snap.ARPVersionCountByPolicy)
	b.flowVersionCounters = nonNilIntMap(snap.FlowVersionCounters)
	b.promptVersionCounters = nonNilIntMap(snap.PromptVersionCounters)
	b.agentVersionCounters = nonNilIntMap(snap.AgentVersionCounters)
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
	b.agentCounter = snap.AgentCounter
	b.actionGroupCounter = snap.ActionGroupCounter
	b.agentAliasCounter = snap.AgentAliasCounter
	b.kbCounter = snap.KBCounter
	b.dataSourceCounter = snap.DataSourceCounter
	b.ingestionJobCounter = snap.IngestionJobCounter
	b.flowCounter = snap.FlowCounter
	b.flowAliasCounter = snap.FlowAliasCounter
	b.promptCounter = snap.PromptCounter
	b.agentCollabCounter = snap.AgentCollabCounter
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

	preRegisterLazyTables(b, snap.Tables)

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

// Snapshot implements persistence.Persistable by delegating to the backend.
// AgentsHandler wraps its own, separate InMemoryBackend instance from
// Handler (see provider.go's Provider vs AgentsProvider), so it needs its own
// delegation rather than sharing Handler's.
func (h *AgentsHandler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *AgentsHandler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
