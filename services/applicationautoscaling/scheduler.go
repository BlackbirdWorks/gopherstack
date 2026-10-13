package applicationautoscaling

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const defaultSchedulerTick = 10 * time.Second

// Scheduler fires due scheduled actions on a ticker.
type Scheduler struct {
	backend *InMemoryBackend
	tick    time.Duration
}

// NewScheduler returns a scheduler for backend; a zero tick uses the default.
func NewScheduler(backend *InMemoryBackend, tick time.Duration) *Scheduler {
	if tick <= 0 {
		tick = defaultSchedulerTick
	}

	return &Scheduler{backend: backend, tick: tick}
}

// Run implements worker.Runner.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.backend.FireDueScheduledActions(ctx, now)
		}
	}
}

type dueAction struct {
	action ScheduledAction
	target ScalableTarget
}

// FireDueScheduledActions applies every scheduled action whose occurrence fell in
// (last evaluation, now], updating the target's min/max capacity.
func (b *InMemoryBackend) FireDueScheduledActions(ctx context.Context, now time.Time) {
	for _, d := range b.collectDueActions(now) {
		b.applyScheduledAction(ctx, d, now)
	}
}

func (b *InMemoryBackend) collectDueActions(now time.Time) []dueAction {
	b.mu.Lock("collectDueActions")
	defer b.mu.Unlock()

	var due []dueAction

	for _, a := range b.scheduledActions.All() {
		baseline, seen := b.lastFired[a.ARN]
		if !seen {
			baseline = a.LastModifiedTime
			if b.startedAt.After(baseline) {
				baseline = b.startedAt
			}
		}

		b.lastFired[a.ARN] = now

		t, ok := b.scalableTargets.Get(scalableTargetKey(a.ServiceNamespace, a.ResourceID, a.ScalableDimension))
		if !ok || !hasCapacityChange(a.ScalableTargetAction) ||
			(t.SuspendedState != nil && t.SuspendedState.ScheduledScalingSuspended) {
			continue
		}

		if scheduledActionDue(a, baseline, now) {
			due = append(due, dueAction{action: *a, target: *t})
		}
	}

	return due
}

func scheduledActionDue(a *ScheduledAction, baseline, now time.Time) bool {
	loc := time.UTC
	if a.Timezone != "" {
		if l, err := time.LoadLocation(a.Timezone); err == nil {
			loc = l
		}
	}

	spec, err := parseSchedule(a.Schedule, loc)
	if err != nil {
		return false
	}

	inWindow := func(t time.Time) bool {
		return (a.StartTime == nil || !t.Before(*a.StartTime)) && (a.EndTime == nil || !t.After(*a.EndTime))
	}

	switch spec.kind {
	case scheduleKindAt:
		return spec.at.After(baseline) && !spec.at.After(now) && inWindow(spec.at)
	case scheduleKindRate:
		first := a.LastModifiedTime
		if a.StartTime != nil {
			first = *a.StartTime
		}

		if now.Before(first) {
			return false
		}

		latest := first.Add(now.Sub(first).Truncate(spec.interval))

		return latest.After(baseline) && inWindow(latest)
	default:
		return cronDue(spec, baseline, now, loc, inWindow)
	}
}

func cronDue(spec scheduleSpec, baseline, now time.Time, loc *time.Location, inWindow func(time.Time) bool) bool {
	const maxScan = 7 * 24 * 60

	start := baseline.In(loc).Truncate(time.Minute).Add(time.Minute)
	end := now.In(loc).Truncate(time.Minute)

	for i, t := 0, start; !t.After(end) && i < maxScan; i, t = i+1, t.Add(time.Minute) {
		if spec.matchesMinute(t) && inWindow(t) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) applyScheduledAction(ctx context.Context, d dueAction, now time.Time) {
	act := d.action.ScalableTargetAction
	newMin, newMax := d.target.MinCapacity, d.target.MaxCapacity

	if act.MinCapacity != nil {
		newMin = *act.MinCapacity
	}

	if act.MaxCapacity != nil {
		newMax = *act.MaxCapacity
	}

	status, message := activityStatusSuccessful, ""

	if newMin > newMax {
		status = "Failed"
		message = "Minimum capacity cannot be greater than maximum capacity."
	} else if d.target.ServiceNamespace == dynamoDBServiceNamespace {
		minC, maxC := newMin, newMax
		if err := b.registerDynamoDBScalableTarget(
			d.target.ResourceID, d.target.ScalableDimension, &minC, &maxC, "",
		); err != nil {
			logger.Load(ctx).WarnContext(ctx, "applicationautoscaling: dynamodb push failed",
				"action", d.action.ScheduledActionName, "error", err)

			status, message = "Failed", err.Error()
		}
	}

	b.mu.Lock("applyScheduledAction")
	defer b.mu.Unlock()

	desc := scheduledActionDescription(act)

	if status == activityStatusSuccessful {
		if t, ok := b.scalableTargets.Get(scalableTargetKey(
			d.target.ServiceNamespace, d.target.ResourceID, d.target.ScalableDimension,
		)); ok {
			t.MinCapacity, t.MaxCapacity, t.LastModifiedTime = newMin, newMax, now.UTC()
		}

		message = "Successfully set " + strings.TrimPrefix(desc, "Setting ")
	}

	b.scalingActivities = append(b.scalingActivities, &ScalingActivity{
		ActivityID:        uuid.NewString(),
		ServiceNamespace:  d.target.ServiceNamespace,
		ResourceID:        d.target.ResourceID,
		ScalableDimension: d.target.ScalableDimension,
		Description:       desc,
		Cause:             "scheduled action name " + d.action.ScheduledActionName + " was triggered",
		StartTime:         now.UTC(),
		EndTime:           now.UTC(),
		StatusCode:        status,
		StatusMessage:     message,
	})
}

func scheduledActionDescription(act *ScalableTargetAction) string {
	switch {
	case act.MinCapacity != nil && act.MaxCapacity != nil:
		return "Setting min capacity to " + strconv.Itoa(
			int(*act.MinCapacity),
		) + " and max capacity to " + strconv.Itoa(
			int(*act.MaxCapacity),
		)
	case act.MinCapacity != nil:
		return "Setting min capacity to " + strconv.Itoa(int(*act.MinCapacity))
	default:
		return "Setting max capacity to " + strconv.Itoa(int(*act.MaxCapacity))
	}
}

func hasCapacityChange(a *ScalableTargetAction) bool {
	return a != nil && (a.MinCapacity != nil || a.MaxCapacity != nil)
}
