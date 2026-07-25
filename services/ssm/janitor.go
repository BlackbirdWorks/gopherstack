package ssm

import (
	"context"
	"encoding/json"
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
	g.Ticker("ParameterExpirer", j.Interval, j.TaskTimeout, j.sweepExpiredParameters)
	g.Ticker("SessionSweeper", j.Interval, j.TaskTimeout, j.sweepTerminatedSessions)
	g.Ticker("ParameterPolicyNotifier", j.Interval, j.TaskTimeout, j.sweepParameterPolicyNotifications)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
//
// sweepParameterPolicyNotifications runs before sweepExpiredParameters so an
// ExpirationNotification due in the same tick a parameter's Expiration
// policy also becomes due still gets reported before the parameter (and its
// policy-notification dedupe state) is deleted.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredCommands(ctx)
	j.sweepParameterPolicyNotifications(ctx)
	j.sweepExpiredParameters(ctx)
	j.sweepTerminatedSessions(ctx)
}

// sweepTerminatedSessions evicts terminated (history) sessions whose EndDate is
// older than the retention window, preventing unbounded growth of the sessions
// store while preserving recent history for DescribeSessions.
func (j *Janitor) sweepTerminatedSessions(ctx context.Context) {
	b := j.Backend
	cutoff := UnixTimeFloat(time.Now()) - sessionHistoryRetentionSecs

	b.mu.Lock("SSMJanitorSession")

	type expiredSession struct {
		region string
		id     string
	}
	var expired []expiredSession

	for region, sessions := range b.sessions {
		for _, s := range sessions.All() {
			if s.Status == sessionStatusTerminated && s.EndDate > 0 && s.EndDate < cutoff {
				expired = append(expired, expiredSession{region: region, id: s.SessionID})
			}
		}
	}

	for _, e := range expired {
		b.sessions[e.region].Delete(e.id)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("ssm", "SessionSweeper", count)
	telemetry.RecordWorkerTask("ssm", "SessionSweeper", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: terminated sessions evicted", "count", count)
	}
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
		for _, cmd := range commands.All() {
			if cmd.ExpiresAfter > 0 && cmd.ExpiresAfter < now {
				expired = append(expired, expiredCmd{region: region, id: cmd.CommandID})
			}
		}
	}

	regions := make(map[string]struct{}, len(expired))
	for _, e := range expired {
		b.commands[e.region].Delete(e.id)
		delete(b.commandInvocations[e.region], e.id)
		regions[e.region] = struct{}{}
	}

	for region := range regions {
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

// parameterExpirationPolicy is the JSON shape of an Expiration policy attached
// to an SSM parameter via PutParameter.Policies.
// AWS policy text format:
//
//	[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"2023-12-01T00:00:00.000Z"}}]
type parameterExpirationPolicy struct {
	Type       string `json:"Type"`
	Attributes struct {
		Timestamp string `json:"Timestamp"`
	} `json:"Attributes"`
}

// sweepExpiredParameters removes parameters that have an Expiration policy whose
// Timestamp has passed.
func (j *Janitor) sweepExpiredParameters(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()

	b.mu.Lock("SSMJanitorParam")

	type expiredParam struct {
		region string
		name   string
	}
	var expired []expiredParam

	for region, params := range b.parameters {
		for _, p := range params.All() {
			if t, ok := parameterExpiresAt(p.Policies); ok && now.After(t) {
				expired = append(expired, expiredParam{region: region, name: p.Name})
			}
		}
	}

	for _, e := range expired {
		b.parameters[e.region].Delete(e.name)
		delete(b.history[e.region], e.name)
		delete(b.parameterLabels[e.region], e.name)
		b.clearParameterPolicyNotificationStateLocked(e.region, e.name)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("ssm", "ParameterExpirer", count)
	telemetry.RecordWorkerTask("ssm", "ParameterExpirer", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: expired parameters evicted", "count", count)
	}
}

// parameterExpiresAt parses the Policies JSON string and returns the expiry time
// from the first Expiration policy found. Returns (zero, false) when no expiry is set.
func parameterExpiresAt(policiesJSON string) (time.Time, bool) {
	if policiesJSON == "" {
		return time.Time{}, false
	}

	var policies []parameterExpirationPolicy
	if err := json.Unmarshal([]byte(policiesJSON), &policies); err != nil {
		return time.Time{}, false
	}

	for _, p := range policies {
		if p.Type != "Expiration" {
			continue
		}

		t, err := time.Parse(time.RFC3339, p.Attributes.Timestamp)
		if err != nil {
			// Try millisecond variant AWS uses
			t, err = time.Parse("2006-01-02T15:04:05.000Z", p.Attributes.Timestamp)
			if err != nil {
				continue
			}
		}

		return t.UTC(), true
	}

	return time.Time{}, false
}

// sweepParameterPolicyNotifications evaluates every parameter's
// ExpirationNotification/NoChangeNotification policies and reports any that
// have newly become due via the injected ParameterPolicyNotifier (see
// parameter_policy_notifications.go). A nil notifier (not yet configured)
// makes this a cheap no-op -- nothing is evaluated or marked notified, so
// once a real notifier is injected later, currently-due policies are still
// reported rather than lost.
func (j *Janitor) sweepParameterPolicyNotifications(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()

	b.mu.Lock("SSMJanitorParamPolicyNotify")

	notifier := b.parameterPolicyNotifier
	if notifier == nil {
		b.mu.Unlock()

		return
	}

	due := b.collectDueParameterPolicyNotificationsLocked(now)

	b.mu.Unlock()

	count := 0

	for _, d := range due {
		if err := notifier.NotifyParameterPolicyAction(ctx, d.parameterName, d.policyType); err != nil {
			logger.Load(ctx).WarnContext(ctx, "SSM janitor: parameter policy notification failed",
				"parameter", d.parameterName, "policyType", d.policyType, "region", d.region, "error", err)

			continue
		}

		count++
	}

	telemetry.RecordWorkerItems("ssm", "ParameterPolicyNotifier", count)
	telemetry.RecordWorkerTask("ssm", "ParameterPolicyNotifier", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SSM janitor: parameter policy notifications sent", "count", count)
	}
}
