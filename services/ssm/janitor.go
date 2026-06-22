package ssm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const defaultSSMJanitorInterval = 30 * time.Second

// Janitor is the SSM background worker that evicts expired commands and their
// invocations to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new SSM Janitor for the given backend.
// If interval is zero it falls back to defaultSSMJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSSMJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "ssm")
	g.Ticker("CommandSweeper", j.Interval, j.TaskTimeout, j.sweepExpiredCommands)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredCommands(ctx)
}

// sweepExpiredCommands removes commands whose ExpiresAfter timestamp has passed,
// together with their associated invocations.
func (j *Janitor) sweepExpiredCommands(ctx context.Context) {
	b := j.Backend
	now := UnixTimeFloat(time.Now())

	b.mu.Lock("SSMJanitor")

	type expiredCmd struct {
		region string
		id     string
	}
	var expired []expiredCmd

	for region, commands := range b.commands {
		for id, cmd := range commands {
			if cmd.ExpiresAfter > 0 && cmd.ExpiresAfter < now {
				expired = append(expired, expiredCmd{region: region, id: id})
			}
		}
	}

	regions := make(map[string]struct{}, len(expired))
	for _, e := range expired {
		delete(b.commands[e.region], e.id)
		delete(b.commandInvocations[e.region], e.id)
		regions[e.region] = struct{}{}
	}

	for region := range regions {
		cleanupEmptyInnerMap(b.commands, region)
		cleanupEmptyInnerMap(b.commandInvocations, region)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("ssm", "CommandSweeper", count)
	telemetry.RecordWorkerTask("ssm", "CommandSweeper", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: expired commands evicted", "count", count)
	}
}
