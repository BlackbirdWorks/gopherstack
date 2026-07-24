package autoscaling

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultScheduledActionTickInterval is how often the scheduler evaluates
// scheduled actions. AWS's own Recurrence cron format is minute-resolution, so
// there is no benefit to ticking faster than once a minute.
const defaultScheduledActionTickInterval = time.Minute

// ScheduledActionScheduler evaluates every Auto Scaling group's scheduled
// actions on a regular tick and applies any that are due, closing the gap
// documented in PARITY.md / bd gopherstack-6ys: PutScheduledUpdateGroupAction
// and BatchPutScheduledUpdateGroupAction persisted StartTime/EndTime/Recurrence,
// but nothing ever evaluated them against wall-clock time.
type ScheduledActionScheduler struct {
	backend      *InMemoryBackend
	tickInterval time.Duration
}

// NewScheduledActionScheduler creates a scheduler for backend. A zero
// tickInterval uses defaultScheduledActionTickInterval.
func NewScheduledActionScheduler(backend *InMemoryBackend, tickInterval time.Duration) *ScheduledActionScheduler {
	if tickInterval <= 0 {
		tickInterval = defaultScheduledActionTickInterval
	}

	return &ScheduledActionScheduler{backend: backend, tickInterval: tickInterval}
}

// Run implements pkgs/worker.Runner: it ticks until ctx is cancelled, applying
// any scheduled actions that have become due on each tick. Every tick is
// independent (no per-tick goroutine, no unbounded state) so Run returns
// promptly once ctx is done, with nothing left running behind it.
func (s *ScheduledActionScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			s.backend.applyDueScheduledActions(ctx, tick, s.tickInterval)
		}
	}
}

// applyDueScheduledActions evaluates every persisted scheduled action against
// now and applies (mutates group capacity for) any that are due. Errors
// applying an individual action are logged and skipped -- one misconfigured
// or now-orphaned scheduled action must never stop the rest of the group's
// actions, or any other group's, from firing. tickInterval seeds the search
// baseline for actions that have never fired; see scheduledActionDue.
func (b *InMemoryBackend) applyDueScheduledActions(ctx context.Context, now time.Time, tickInterval time.Duration) {
	b.mu.Lock("applyDueScheduledActions")
	defer b.mu.Unlock()

	for _, a := range b.scheduledActions.All() {
		if !scheduledActionDue(a, now, tickInterval) {
			continue
		}

		b.fireScheduledActionLocked(ctx, a, now)
	}
}

// scheduledActionDue reports whether a's next occurrence has arrived as of
// now. tickInterval seeds the search baseline for an action that has never
// fired, so a freshly created action only considers occurrences within the
// upcoming tick window rather than backfilling every occurrence since
// StartTime (which, for a long-lived daily/hourly schedule created years
// after StartTime, could otherwise mean firing thousands of times on the very
// next tick).
func scheduledActionDue(a *ScheduledAction, now time.Time, tickInterval time.Duration) bool {
	if !a.StartTime.IsZero() && now.Before(a.StartTime) {
		return false
	}

	if a.Recurrence == "" {
		// One-time action: fires exactly once, at/after StartTime.
		return !a.StartTime.IsZero() && a.LastExecutedTime.IsZero()
	}

	if !a.EndTime.IsZero() && now.After(a.EndTime) {
		return false
	}

	schedule, err := parseRecurrence(a.Recurrence)
	if err != nil {
		return false
	}

	baseline := a.LastExecutedTime
	if baseline.IsZero() {
		baseline = now.Add(-tickInterval)
	}

	next := schedule.NextAfter(baseline)

	return !next.After(now)
}

// fireScheduledActionLocked applies a's MinSize/MaxSize/DesiredCapacity to its
// group (reusing the same validated capacity-update path UpdateAutoScalingGroup
// uses) and records LastExecutedTime so scheduledActionDue does not refire the
// same occurrence on the next tick. Must be called with b.mu held.
func (b *InMemoryBackend) fireScheduledActionLocked(ctx context.Context, a *ScheduledAction, now time.Time) {
	// Always stamp LastExecutedTime, even on failure: a scheduled action whose
	// group vanished or whose bounds are now inconsistent will fail identically
	// on every future tick, and re-attempting it every minute forever would be
	// a silent busy-loop, not a retry with any chance of succeeding differently.
	defer func() { a.LastExecutedTime = now }()

	g, ok := b.groups.Get(a.AutoScalingGroupName)
	if !ok {
		return
	}

	update := UpdateAutoScalingGroupInput{
		MinSize:         a.MinSize,
		MaxSize:         a.MaxSize,
		DesiredCapacity: a.DesiredCapacity,
	}

	if err := b.applyUpdateCapacityLocked(g, update); err != nil {
		logger.Load(ctx).WarnContext(ctx, "autoscaling: scheduled action failed to apply",
			"group", a.AutoScalingGroupName, "action", a.ScheduledActionName, "error", err)

		return
	}

	b.activities[a.AutoScalingGroupName] = append(b.activities[a.AutoScalingGroupName], ScalingActivity{
		ActivityID:           uuid.NewString(),
		AutoScalingGroupName: a.AutoScalingGroupName,
		Description:          "Scheduled action \"" + a.ScheduledActionName + "\" applied capacity change",
		StatusCode:           statusCodeSuccessful,
		Progress:             completedProgress,
		StartTime:            now,
		EndTime:              now,
	})
}
