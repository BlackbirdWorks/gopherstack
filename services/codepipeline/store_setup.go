package codepipeline

// Code in this file supports Phase 3.3 of the datalayer refactor: the five
// region-nested resource collections (previously map[string]map[K]*V, outer
// key = region) are replaced by flat *store.Table[V] values, each keyed by a
// composite "region|id" string (see regionKey in backend.go), with companion
// *store.Index values replacing the old per-region iteration and reverse-ARN
// lookups. It otherwise follows pkgs/store's package doc and the
// services/mwaa (region-qualified-table) conversion.
//
// executions and actionExecutions (map[string]map[string][]*T) are
// deliberately NOT converted: store.Table requires a *V value with its own
// identity, but each entry here is a bare []*T slice with no identifier of
// its own. They remain plain nested maps, unchanged by this refactor -- see
// executionsStore/actionExecutionsStore in backend.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// pipelineKeyFn derives the composite primary key for the flat pipelines
// table from the region-qualifying region field and the pipeline's own
// declared name.
func pipelineKeyFn(v *Pipeline) string { return regionKey(v.region, v.Declaration.Name) }

// pipelineRegionIndexKeyFn groups pipelines by region alone, replacing the
// old outer map[region] nesting for per-region scans (ListPipelines,
// DeleteCustomActionType's in-use check).
func pipelineRegionIndexKeyFn(v *Pipeline) string { return v.region }

// pipelineARNIndexKeyFn groups pipelines by "region|ARN", replacing the old
// per-region ARN reverse map (region → ARN → name) used by resolveResourceARN.
func pipelineARNIndexKeyFn(v *Pipeline) string { return regionKey(v.region, v.Metadata.PipelineArn) }

// customActionTypeKeyFn derives the composite primary key for the flat
// customActionTypes table from the region and the type's own
// category/provider/version identity (see customActionTypeKey.String).
func customActionTypeKeyFn(v *CustomActionType) string {
	key := customActionTypeKey{Category: v.Category, Provider: v.Provider, Version: v.Version}

	return regionKey(v.region, key.String())
}

// customActionTypeRegionIndexKeyFn groups custom action types by region
// alone, replacing the old outer map[region] nesting used by ListActionTypes.
func customActionTypeRegionIndexKeyFn(v *CustomActionType) string { return v.region }

// jobKeyFn derives the composite primary key for the flat jobs table from the
// region and the job's own ID.
func jobKeyFn(v *Job) string { return regionKey(v.region, v.ID) }

// jobRegionIndexKeyFn groups jobs by region alone, replacing the old outer
// map[region] nesting used by PollForJobs.
func jobRegionIndexKeyFn(v *Job) string { return v.region }

// webhookKeyFn derives the composite primary key for the flat webhooks table
// from the region and the webhook's own name.
func webhookKeyFn(v *Webhook) string { return regionKey(v.region, v.Name) }

// webhookRegionIndexKeyFn groups webhooks by region alone, replacing the old
// outer map[region] nesting used by ListWebhooks.
func webhookRegionIndexKeyFn(v *Webhook) string { return v.region }

// webhookARNIndexKeyFn groups webhooks by "region|ARN", replacing the old
// per-region ARN reverse map (region → ARN → name) used by resolveResourceARN.
func webhookARNIndexKeyFn(v *Webhook) string { return regionKey(v.region, v.ARN) }

// stageTransitionKeyFn derives the composite primary key for the flat
// stageTransitions table from the region and the transition's own
// pipeline/stage/type identity (see stageTransitionKey.String).
func stageTransitionKeyFn(v *StageTransitionState) string {
	key := stageTransitionKey{PipelineName: v.PipelineName, StageName: v.StageName, TransitionType: v.TransitionType}

	return regionKey(v.region, key.String())
}

// stageTransitionPipelineIndexKeyFn groups stage transitions by
// "region|pipelineName", replacing the old linear scan over the per-region
// map used by DeletePipeline's cascade delete.
func stageTransitionPipelineIndexKeyFn(v *StageTransitionState) string {
	return regionKey(v.region, v.PipelineName)
}

// registerAllTables registers the backend's five resource tables exactly
// once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.pipelines = store.Register(b.registry, "pipelines", store.New(pipelineKeyFn))
	b.pipelinesByRegion = b.pipelines.AddIndex("byRegion", pipelineRegionIndexKeyFn)
	b.pipelinesByARN = b.pipelines.AddIndex("byARN", pipelineARNIndexKeyFn)

	b.customActionTypes = store.Register(b.registry, "customActionTypes", store.New(customActionTypeKeyFn))
	b.customActionTypesByRegion = b.customActionTypes.AddIndex("byRegion", customActionTypeRegionIndexKeyFn)

	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.jobsByRegion = b.jobs.AddIndex("byRegion", jobRegionIndexKeyFn)

	b.webhooks = store.Register(b.registry, "webhooks", store.New(webhookKeyFn))
	b.webhooksByRegion = b.webhooks.AddIndex("byRegion", webhookRegionIndexKeyFn)
	b.webhooksByARN = b.webhooks.AddIndex("byARN", webhookARNIndexKeyFn)

	b.stageTransitions = store.Register(b.registry, "stageTransitions", store.New(stageTransitionKeyFn))
	b.stageTransitionsByPipeline = b.stageTransitions.AddIndex("byPipeline", stageTransitionPipelineIndexKeyFn)
}
