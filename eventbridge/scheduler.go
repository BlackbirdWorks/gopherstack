package eventbridge

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const defaultSchedulerTickInterval = time.Minute

// Scheduler fires EventBridge scheduled rules on a regular tick interval.
// It evaluates all ENABLED rules with a ScheduleExpression and calls PutEvents
// for any rule whose next fire time has passed since the last tick.
type Scheduler struct {
	backend      *InMemoryBackend
	logger       *slog.Logger
	tickInterval time.Duration
}

// NewScheduler creates a new Scheduler backed by the given InMemoryBackend.
func NewScheduler(backend *InMemoryBackend, log *slog.Logger, tickInterval time.Duration) *Scheduler {
	if tickInterval <= 0 {
		tickInterval = defaultSchedulerTickInterval
	}

	return &Scheduler{
		backend:      backend,
		logger:       log,
		tickInterval: tickInterval,
	}
}

// Run runs the scheduler until ctx is cancelled.
// Renamed from Start → Run to match the janitor.Run convention used by other workers.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	// Track the last fire time per rule ARN to avoid duplicate fires.
	lastFired := make(map[string]time.Time)
	now := time.Now()

	// Initialize lastFired to "now" so we don't fire all rules immediately on start.
	s.initLastFired(lastFired, now)

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			s.processTick(ctx, tick, lastFired)
		}
	}
}

// initLastFired seeds the lastFired map with the current time for all scheduled rules.
func (s *Scheduler) initLastFired(lastFired map[string]time.Time, now time.Time) {
	s.backend.mu.RLock()
	defer s.backend.mu.RUnlock()

	for _, busRules := range s.backend.rules {
		for _, rule := range busRules {
			if rule.ScheduleExpression != "" && rule.State == ruleStateEnabled {
				lastFired[rule.Arn] = now
			}
		}
	}
}

// processTick evaluates all scheduled rules and fires any that are due.
func (s *Scheduler) processTick(ctx context.Context, tick time.Time, lastFired map[string]time.Time) {
	s.backend.mu.RLock()
	type ruleInfo struct {
		rule    Rule
		busName string
	}

	var scheduled []ruleInfo
	for busName, busRules := range s.backend.rules {
		for _, rule := range busRules {
			if rule.ScheduleExpression != "" && rule.State == ruleStateEnabled {
				scheduled = append(scheduled, ruleInfo{rule: *rule, busName: busName})
			}
		}
	}
	s.backend.mu.RUnlock()

	fired := 0

	for _, info := range scheduled {
		rule := info.rule
		expr, err := parseScheduleExpression(rule.ScheduleExpression)
		if err != nil {
			s.logger.WarnContext(ctx, "EventBridge: failed to parse schedule expression",
				"rule", rule.Name, "expr", rule.ScheduleExpression, "error", err)
			telemetry.RecordWorkerTask("eventbridge", "Scheduler", "error")

			continue
		}

		last, ok := lastFired[rule.Arn]
		if !ok {
			last = tick.Add(-s.tickInterval)
		}

		// Fire the rule for each tick window that has passed.
		next := expr.NextAfter(last)
		if next.Before(tick) || next.Equal(tick) {
			s.fireRule(ctx, rule, info.busName)
			lastFired[rule.Arn] = tick
			fired++
		}
	}

	// Record one task observation per tick (even if no rules fired).
	telemetry.RecordWorkerTask("eventbridge", "Scheduler", "success")

	if fired > 0 {
		telemetry.RecordWorkerItems("eventbridge", "Scheduler", fired)
	}
}

// fireRule synthesizes a scheduled event and calls PutEvents.
func (s *Scheduler) fireRule(ctx context.Context, rule Rule, busName string) {
	s.logger.DebugContext(ctx, "EventBridge: firing scheduled rule", "rule", rule.Name, "bus", busName)

	detail := `{"scheduled":true}`
	sourceFromExpr := "aws.events"

	// Detect cron vs rate for detail-type.
	detailType := "Scheduled Event"
	if strings.HasPrefix(rule.ScheduleExpression, "cron(") {
		detailType = "Scheduled Event (cron)"
	}

	entry := EventEntry{
		Source:       sourceFromExpr,
		DetailType:   detailType,
		Detail:       detail,
		EventBusName: busName,
	}

	s.backend.PutEvents([]EventEntry{entry})
}
