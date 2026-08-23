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
)

// RunJanitor periodically advances time-based state machines for provisioned resources.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	g := worker.NewGroup(ctx, "bedrock")
	g.Ticker("Janitor", interval, 0, b.runJanitorTick)

	<-ctx.Done()
	g.Stop()
}

func (b *InMemoryBackend) runJanitorTick(ctx context.Context) {
	log := logger.Load(ctx)

	if n := b.AdvanceProvisionedModelThroughputStatuses(); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "PMTAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced PMT statuses", "count", n)
	}

	if n := b.AdvanceCustomModelDeploymentStatuses(); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CustomModelDeploymentAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced custom model deployment statuses", "count", n)
	}

	if n := b.AdvanceCustomizationJobStatuses(defaultJobCompletionDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CustomizationJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced customization job statuses", "count", n)
	}

	if n := b.AdvanceCopyImportJobStatuses(defaultJobCompletionDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "CopyImportJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced copy/import job statuses", "count", n)
	}

	if n := b.AdvanceAdvancedPromptOptimizationJobStatuses(defaultJobCompletionDelay); n > 0 {
		telemetry.RecordWorkerItems("bedrock", "AdvancedPromptOptimizationJobAdvancer", n)
		log.DebugContext(ctx, "bedrock janitor: advanced advanced-prompt-optimization job statuses", "count", n)
	}

	telemetry.RecordWorkerTask("bedrock", "Janitor", "success")
}
