package ssm

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			taskCtx, cancel := j.taskContext(ctx)
			j.SweepOnce(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredCommands(ctx)
	j.sweepExpiredParameters(ctx)
}

// sweepExpiredCommands removes commands whose ExpiresAfter timestamp has passed,
// together with their associated invocations.
func (j *Janitor) sweepExpiredCommands(ctx context.Context) {
	b := j.Backend
	now := UnixTimeFloat(time.Now())

	b.mu.Lock("SSMJanitorCommands")

	var expired []string

	for id, cmd := range b.commands {
		if cmd.ExpiresAfter > 0 && cmd.ExpiresAfter < now {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		delete(b.commands, id)
		delete(b.commandInvocations, id)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("ssm", "CommandSweeper", count)
	telemetry.RecordWorkerTask("ssm", "CommandSweeper", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: expired commands evicted", "count", count)
	}
}

// sweepExpiredParameters removes parameters whose Expiration lifecycle policy
// timestamp has passed.
func (j *Janitor) sweepExpiredParameters(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()

	b.mu.Lock("SSMJanitorParameters")

	var expired []string

	for name, param := range b.parameters {
		if param.Policies == "" {
			continue
		}

		if parameterHasExpired(param.Policies, now) {
			expired = append(expired, name)
		}
	}

	for _, name := range expired {
		delete(b.parameters, name)
		delete(b.history, name)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("ssm", "ParameterExpirer", count)
	telemetry.RecordWorkerTask("ssm", "ParameterExpirer", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: expired parameters evicted", "count", count)
	}
}

// parameterHasExpired returns true if any Expiration policy in the JSON policies
// string has a timestamp that is in the past.
func parameterHasExpired(policies string, now time.Time) bool {
	var pols []ParameterPolicy
	if err := json.Unmarshal([]byte(policies), &pols); err != nil {
		return false
	}

	for _, pol := range pols {
		if pol.Type != PolicyTypeExpiration {
			continue
		}

		if pol.Attributes.Timestamp == "" {
			continue
		}

		ts, err := time.Parse(time.RFC3339, pol.Attributes.Timestamp)
		if err != nil {
			continue
		}

		if now.After(ts) {
			return true
		}
	}

	return false
}
