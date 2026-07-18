// Package codepipeline provides an in-memory implementation of the AWS CodePipeline service.
package codepipeline

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// CodePipeline resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store. Pipelines, action types, jobs, webhooks, executions, and stage
// transitions are all region-scoped in AWS, so cross-region references never occur
// and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	// statusInProgress is the status for an in-progress job or execution.
	statusInProgress = "InProgress"

	// PipelineTypeV1 and PipelineTypeV2 are the valid PipelineType values.
	PipelineTypeV1 = "V1"
	PipelineTypeV2 = "V2"

	// ExecutionModeQueued is the QUEUED execution mode.
	ExecutionModeQueued = "QUEUED"
	// ExecutionModeSuperseded is the SUPERSEDED execution mode.
	ExecutionModeSuperseded = "SUPERSEDED"
	// ExecutionModeParallel is the PARALLEL execution mode.
	ExecutionModeParallel = "PARALLEL"

	// WebhookAuthGitHubHMAC is the GITHUB_HMAC authentication type for webhooks.
	WebhookAuthGitHubHMAC = "GITHUB_HMAC"
	// WebhookAuthIP is the IP authentication type for webhooks.
	WebhookAuthIP = "IP"
	// WebhookAuthUnauthenticated is the UNAUTHENTICATED authentication type for webhooks.
	WebhookAuthUnauthenticated = "UNAUTHENTICATED"

	// kindPipeline is the resource kind string for pipelines.
	kindPipeline = "pipeline"
	// kindWebhook is the resource kind string for webhooks.
	kindWebhook = "webhook"

	// keyPipelineExecutionID and keyStatus are JSON keys shared across the
	// execution-detail response maps.
	keyPipelineExecutionID = "pipelineExecutionId"
	keyStatus              = "status"

	// statusSucceeded is the terminal success status for executions and actions.
	statusSucceeded = "Succeeded"

	// statusStopped is the terminal status for a manually stopped pipeline execution.
	statusStopped = "Stopped"

	// ruleOwnerAWS is the owner value for AWS-managed CodePipeline rule types.
	ruleOwnerAWS = "AWS"
)

// InMemoryBackend is a thread-safe in-memory store for CodePipeline resources.
//
// pipelines, customActionTypes, jobs, webhooks, and stageTransitions are flat
// store.Table collections keyed by a composite "region|id" string (see
// regionKey below), replacing the old map[string]map[K]*V nesting (outer key
// = region) that isolated same-named resources across regions. Each table's
// companion *store.Index values replace the old per-region
// iteration/reverse-ARN-map lookups -- see store_setup.go. executions and
// actionExecutions remain plain region-nested maps: their values are bare
// []*T slices with no identity of their own, so they are not candidates for
// store.Table (see pkgs/store's package doc). Callers must hold b.mu while
// accessing any of these collections.
type InMemoryBackend struct {
	pipelines                  *store.Table[Pipeline]
	pipelinesByRegion          *store.Index[Pipeline]
	pipelinesByARN             *store.Index[Pipeline]
	customActionTypes          *store.Table[CustomActionType]
	customActionTypesByRegion  *store.Index[CustomActionType]
	jobs                       *store.Table[Job]
	jobsByRegion               *store.Index[Job]
	webhooks                   *store.Table[Webhook]
	webhooksByRegion           *store.Index[Webhook]
	webhooksByARN              *store.Index[Webhook]
	stageTransitions           *store.Table[StageTransitionState]
	stageTransitionsByPipeline *store.Index[StageTransitionState]
	registry                   *store.Registry
	executions                 map[string]map[string][]*PipelineExecution // region → pipelineName → executions
	actionExecutions           map[string]map[string][]*ActionExecution   // region → pipelineName → action executions
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:         store.NewRegistry(),
		executions:       make(map[string]map[string][]*PipelineExecution),
		actionExecutions: make(map[string]map[string][]*ActionExecution),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("codepipeline-" + region),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every resource table this backend owns.
func regionKey(region, id string) string { return region + "|" + id }

// executionsStore returns the per-region execution-history map for region,
// lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) executionsStore(region string) map[string][]*PipelineExecution {
	if b.executions[region] == nil {
		b.executions[region] = make(map[string][]*PipelineExecution)
	}

	return b.executions[region]
}

// actionExecutionsStore returns the per-region action-execution map for
// region, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) actionExecutionsStore(region string) map[string][]*ActionExecution {
	if b.actionExecutions[region] == nil {
		b.actionExecutions[region] = make(map[string][]*ActionExecution)
	}

	return b.actionExecutions[region]
}

// Region returns the default region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.executions = make(map[string]map[string][]*PipelineExecution)
	b.actionExecutions = make(map[string]map[string][]*ActionExecution)
}

func (b *InMemoryBackend) buildPipelineARN(region, name string) string {
	return arn.Build("codepipeline", region, b.accountID, name)
}

func (b *InMemoryBackend) buildWebhookARN(region, name string) string {
	return arn.Build("codepipeline", region, b.accountID, "webhook:"+name)
}
