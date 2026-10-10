package bedrock

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// defaultJobCompletionDelay is how long customization/copy/import jobs stay InProgress.
	defaultJobCompletionDelay = 5 * time.Second

	// defaultJanitorInterval is how often the janitor runs.
	defaultJanitorInterval = 5 * time.Second

	janitorTicksPerDelay = 2
)

// SetJobCompletionDelay overrides how long customization, copy, import,
// evaluation, invocation and prompt-optimization jobs stay in progress; 0
// (the default) keeps the built-in 5s. Call before RunJanitor starts.
func (b *InMemoryBackend) SetJobCompletionDelay(d time.Duration) {
	b.jobDelay.Store(int64(d))
}

func (b *InMemoryBackend) jobCompletionDelay() time.Duration {
	if d := time.Duration(b.jobDelay.Load()); d > 0 {
		return d
	}

	return defaultJobCompletionDelay
}

// RunJanitor periodically advances time-based state machines for provisioned resources.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	if d := b.jobCompletionDelay() / janitorTicksPerDelay; d > 0 && d < interval {
		interval = d
	}

	g := worker.NewGroup(ctx, "bedrock")
	g.Ticker("Janitor", interval, 0, b.runJanitorTick)

	<-ctx.Done()
	g.Stop()
}

func (b *InMemoryBackend) runJanitorTick(ctx context.Context) {
	log := logger.Load(ctx)
	jobDelay := b.jobCompletionDelay()

	if n := b.AdvanceProvisionedModelThroughputStatuses(); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "PMTAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced PMT statuses", "count", n)
	}

	if n := b.AdvanceCustomModelDeploymentStatuses(); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CustomModelDeploymentAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced custom model deployment statuses", "count", n)
	}

	if n := b.AdvanceCustomizationJobStatuses(jobDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CustomizationJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced customization job statuses", "count", n)
	}

	if n := b.AdvanceCopyImportJobStatuses(jobDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CopyImportJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced copy/import job statuses", "count", n)
	}

	if n := b.AdvanceAdvancedPromptOptimizationJobStatuses(jobDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "AdvancedPromptOptimizationJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced advanced-prompt-optimization job statuses", "count", n)
	}

	if n := b.AdvanceEvaluationAndInvocationJobStatuses(jobDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "EvaluationInvocationJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced evaluation/invocation job statuses", "count", n)
	}

	if n := b.SettleStoppingJobs(); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "StoppingJobSettler", n)
		log.DebugContext(ctx, "bedrock janitor: settled stopping jobs", "count", n)
	}

	telemetry.RecordWorkerTask("bedrock", "Janitor", "success")
}
