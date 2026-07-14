package eventbridge

import (
	"context"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const defaultSchedulerTickInterval = time.Minute

// Scheduler fires EventBridge scheduled rules on a regular tick interval.
// It evaluates all ENABLED rules with a ScheduleExpression and calls PutEvents
// for any rule whose next fire time has passed since the last tick.
type Scheduler struct {
	backend      *InMemoryBackend
	tickInterval time.Duration
}

// NewScheduler creates a new Scheduler backed by the given InMemoryBackend.
func NewScheduler(backend *InMemoryBackend, tickInterval time.Duration) *Scheduler {
	if tickInterval <= 0 {
		tickInterval = defaultSchedulerTickInterval
	}

	return &Scheduler{
		backend:      backend,
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
	s.backend.mu.RLock("initLastFired")
	defer s.backend.mu.RUnlock()

	for _, regRules := range s.backend.rules {
		for _, busRules := range regRules {
			for _, rule := range busRules.All() {
				if rule.ScheduleExpression != "" && rule.State == ruleStateEnabled {
					lastFired[rule.Arn] = now
				}
			}
		}
	}
}

// scheduledRuleInfo pairs a scheduled rule snapshot with the bus/region it lives in.
type scheduledRuleInfo struct {
	busName string
	region  string
	rule    Rule
}

// collectScheduledRules snapshots all ENABLED rules with a ScheduleExpression, plus the
// full set of currently-active rule ARNs (used to prune stale lastFired entries).
func (s *Scheduler) collectScheduledRules() ([]scheduledRuleInfo, map[string]struct{}) {
	s.backend.mu.RLock("processTick")
	defer s.backend.mu.RUnlock()

	var scheduled []scheduledRuleInfo
	activeARNs := make(map[string]struct{})
	for region, regRules := range s.backend.rules {
		for busName, busRules := range regRules {
			for _, rule := range busRules.All() {
				if rule.ScheduleExpression != "" && rule.State == ruleStateEnabled {
					scheduled = append(scheduled, scheduledRuleInfo{rule: *rule, busName: busName, region: region})
				}
				activeARNs[rule.Arn] = struct{}{}
			}
		}
	}

	return scheduled, activeARNs
}

// pruneLastFired removes lastFired entries for rules that no longer exist to prevent
// unbounded memory growth.
func pruneLastFired(lastFired map[string]time.Time, activeARNs map[string]struct{}) {
	for arn := range lastFired {
		if _, ok := activeARNs[arn]; !ok {
			delete(lastFired, arn)
		}
	}
}

// fireDueRule fires rule if its next scheduled occurrence has passed as of tick, updating
// lastFired accordingly. Returns true if the rule fired.
func (s *Scheduler) fireDueRule(
	ctx context.Context,
	info scheduledRuleInfo,
	tick time.Time,
	lastFired map[string]time.Time,
) bool {
	rule := info.rule

	expr, err := parseScheduleExpression(rule.ScheduleExpression)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "EventBridge: failed to parse schedule expression",
			"rule", rule.Name, "expr", rule.ScheduleExpression, "error", err)
		telemetry.RecordWorkerTask("eventbridge", "Scheduler", "error")

		return false
	}

	last, ok := lastFired[rule.Arn]
	if !ok {
		last = tick.Add(-s.tickInterval)
	}

	// Fire the rule for each tick window that has passed.
	next := expr.NextAfter(last)
	if next.Before(tick) || next.Equal(tick) {
		s.fireRule(ctx, rule, info.busName, info.region)
		lastFired[rule.Arn] = tick

		return true
	}

	return false
}

// processTick evaluates all scheduled rules and fires any that are due.
func (s *Scheduler) processTick(
	ctx context.Context,
	tick time.Time,
	lastFired map[string]time.Time,
) {
	scheduled, activeARNs := s.collectScheduledRules()
	pruneLastFired(lastFired, activeARNs)

	fired := 0

	for _, info := range scheduled {
		if s.fireDueRule(ctx, info, tick, lastFired) {
			fired++
		}
	}

	// Record one task observation per tick (even if no rules fired).
	telemetry.RecordWorkerTask("eventbridge", "Scheduler", "success")

	if fired > 0 {
		telemetry.RecordWorkerItems("eventbridge", "Scheduler", fired)
	}
}

// fireRule delivers a scheduled event directly to the rule's targets.
// Scheduled rules bypass pattern matching — targets are invoked unconditionally.
func (s *Scheduler) fireRule(ctx context.Context, rule Rule, busName, region string) {
	logger.Load(ctx).DebugContext(ctx, "EventBridge: firing scheduled rule", "rule", rule.Name, "bus", busName)

	detailType := "Scheduled Event"
	if strings.HasPrefix(rule.ScheduleExpression, "cron(") {
		detailType = "Scheduled Event (cron)"
	}

	s.backend.deliverScheduledRule(ctx, rule, busName, region, detailType)
}
