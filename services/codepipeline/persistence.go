package codepipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// codepipelineSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to regionalDTO/backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape (e.g. a
// field's meaning or type changes). Restore compares this against the
// persisted value and discards (rather than attempts to partially decode) any
// mismatch -- see Restore below. This is the first version: earlier
// (pre-Phase-3.3) snapshots carried no version field at all, so they also
// fail this check and are discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const codepipelineSnapshotVersion = 1

// regionalDTO wraps a region-nested resource value for JSON round-tripping
// through store.Registry. Table[V].Snapshot's plain json.Marshal(V) cannot
// see the unexported `region` field each resource type carries (used only for
// the live table's composite key -- see store_setup.go), so it is carried
// alongside Value here instead. ID mirrors the resource's own identity (which
// may itself be a composite of several fields, e.g. customActionTypeKey or
// stageTransitionKey stringified) so the DTO table has a stable composite key
// ("region|id") independent of the concrete resource type.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the [store.Table] key function shared by every DTO
// table below; it mirrors the "region|id" composite key each live table uses.
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// executionEntry is the JSON-serialisable list of executions per pipeline.
// executions is a plain region-nested map (not a store.Table -- see
// store_setup.go), so it is flattened into region-tagged entries here rather
// than going through the DTO registry.
type executionEntry struct {
	Region       string               `json:"region"`
	PipelineName string               `json:"pipelineName"`
	Executions   []*PipelineExecution `json:"executions"`
}

// backendSnapshot is the top-level on-disk shape for the CodePipeline
// backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- pipelines, customActionTypes, jobs,
// webhooks, and stageTransitions, each wrapped in a regionalDTO to carry its
// region field through. Executions is still a region-nested plain map (see
// store_setup.go for why it was not converted to store.Table) and is
// persisted directly via the executionEntry flattening above.
// actionExecutions is derived state (rebuilt on StartPipelineExecution) and
// is deliberately not persisted, matching pre-Phase-3.3 behaviour. Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables     map[string]json.RawMessage `json:"tables"`
	AccountID  string                     `json:"accountID"`
	Region     string                     `json:"region"`
	Executions []executionEntry           `json:"executions"`
	Version    int                        `json:"version"`
}

// customActionTypeKey.String returns a unique string for use as the DTO id
// and as half of the customActionTypes table's composite key.
func (k customActionTypeKey) String() string {
	return k.Category + "/" + k.Provider + "/" + k.Version
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because each DTO value type (regionalDTO[V]) differs
// from its corresponding live table's value type (V).
func buildPersistenceDTORegistry() (
	*store.Registry,
	*store.Table[regionalDTO[Pipeline]],
	*store.Table[regionalDTO[CustomActionType]],
	*store.Table[regionalDTO[Job]],
	*store.Table[regionalDTO[Webhook]],
	*store.Table[regionalDTO[StageTransitionState]],
) {
	dtoReg := store.NewRegistry()
	pipelineDTOs := store.Register(dtoReg, "pipelines", store.New(regionalDTOKeyFn[Pipeline]))
	catDTOs := store.Register(dtoReg, "customActionTypes", store.New(regionalDTOKeyFn[CustomActionType]))
	jobDTOs := store.Register(dtoReg, "jobs", store.New(regionalDTOKeyFn[Job]))
	webhookDTOs := store.Register(dtoReg, "webhooks", store.New(regionalDTOKeyFn[Webhook]))
	transitionDTOs := store.Register(dtoReg, "stageTransitions", store.New(regionalDTOKeyFn[StageTransitionState]))

	return dtoReg, pipelineDTOs, catDTOs, jobDTOs, webhookDTOs, transitionDTOs
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtoReg, pipelineDTOs, catDTOs, jobDTOs, webhookDTOs, transitionDTOs := buildPersistenceDTORegistry()

	for _, v := range b.pipelines.Snapshot() {
		pipelineDTOs.Put(&regionalDTO[Pipeline]{Region: v.region, ID: v.Declaration.Name, Value: v})
	}

	for _, v := range b.customActionTypes.Snapshot() {
		key := customActionTypeKey{Category: v.Category, Provider: v.Provider, Version: v.Version}
		catDTOs.Put(&regionalDTO[CustomActionType]{Region: v.region, ID: key.String(), Value: v})
	}

	for _, v := range b.jobs.Snapshot() {
		jobDTOs.Put(&regionalDTO[Job]{Region: v.region, ID: v.ID, Value: v})
	}

	for _, v := range b.webhooks.Snapshot() {
		webhookDTOs.Put(&regionalDTO[Webhook]{Region: v.region, ID: v.Name, Value: v})
	}

	for _, v := range b.stageTransitions.Snapshot() {
		key := stageTransitionKey{
			PipelineName: v.PipelineName, StageName: v.StageName, TransitionType: v.TransitionType,
		}
		transitionDTOs.Put(&regionalDTO[StageTransitionState]{Region: v.region, ID: key.String(), Value: v})
	}

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx,
			"codepipeline: snapshot table marshal failed", "error", err)

		return nil
	}

	execs := make([]executionEntry, 0)

	for region, inner := range b.executions {
		for pName, list := range inner {
			if len(list) == 0 {
				continue
			}

			execs = append(execs, executionEntry{Region: region, PipelineName: pName, Executions: list})
		}
	}

	snap := backendSnapshot{
		Version:    codepipelineSnapshotVersion,
		Tables:     tables,
		Executions: execs,
		AccountID:  b.accountID,
		Region:     b.region,
	}

	// Marshal can only fail for unsupported types (e.g. channels/functions) which are not present here.
	return persistence.MarshalSnapshot(ctx, "codepipeline", &snap)
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codepipeline", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != codepipelineSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"codepipeline: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", codepipelineSnapshotVersion)

		b.registry.ResetAll()
		b.executions = make(map[string]map[string][]*PipelineExecution)
		b.actionExecutions = make(map[string]map[string][]*ActionExecution)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	executions := make(map[string]map[string][]*PipelineExecution)

	for _, entry := range snap.Executions {
		if executions[entry.Region] == nil {
			executions[entry.Region] = make(map[string][]*PipelineExecution)
		}

		executions[entry.Region][entry.PipelineName] = entry.Executions
	}

	b.executions = executions

	// actionExecutions are derived state (rebuilt on StartPipelineExecution) and
	// are not persisted; ensure the map is initialised after a restore.
	b.actionExecutions = make(map[string]map[string][]*ActionExecution)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreResourceTables rebuilds the five resource store.Table values from
// snap's per-table JSON, factored out of Restore to keep Restore's own
// cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtoReg, pipelineDTOs, catDTOs, jobDTOs, webhookDTOs, transitionDTOs := buildPersistenceDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("codepipeline: restore snapshot tables: %w", err)
	}

	pipelines := make([]*Pipeline, 0, pipelineDTOs.Len())

	for _, d := range pipelineDTOs.All() {
		if d.Value == nil {
			// Not producible by Snapshot, but a defensive guard against a
			// hand-edited or corrupted snapshot.
			continue
		}

		d.Value.region = d.Region
		pipelines = append(pipelines, d.Value)
	}

	b.pipelines.Restore(pipelines)

	cats := make([]*CustomActionType, 0, catDTOs.Len())

	for _, d := range catDTOs.All() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		cats = append(cats, d.Value)
	}

	b.customActionTypes.Restore(cats)

	jobs := make([]*Job, 0, jobDTOs.Len())

	for _, d := range jobDTOs.All() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		jobs = append(jobs, d.Value)
	}

	b.jobs.Restore(jobs)

	webhooks := make([]*Webhook, 0, webhookDTOs.Len())

	for _, d := range webhookDTOs.All() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		webhooks = append(webhooks, d.Value)
	}

	b.webhooks.Restore(webhooks)

	transitions := make([]*StageTransitionState, 0, transitionDTOs.Len())

	for _, d := range transitionDTOs.All() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		transitions = append(transitions, d.Value)
	}

	b.stageTransitions.Restore(transitions)

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
