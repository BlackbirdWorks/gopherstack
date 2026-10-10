package elasticbeanstalk

import (
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func (b *InMemoryBackend) eventsSlice(region string) []*EventRecord {
	if b.events[region] == nil {
		b.events[region] = make([]*EventRecord, 0)
	}

	return b.events[region]
}

// eventsSliceRO returns the region-scoped events slice for region without
// mutating the outer map. Safe to call while holding only b.mu.RLock(): if
// the region has not been observed yet, it returns a fresh, unregistered,
// empty slice instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) eventsSliceRO(region string) []*EventRecord {
	if v := b.events[region]; v != nil {
		return v
	}

	return []*EventRecord{}
}

// appendEvent appends an event record to the backend's event log, capturing
// env's PlatformArn/TemplateName/VersionLabel at the moment of the action
// (real EventDescription.PlatformArn/TemplateName/VersionLabel: "associated
// with this event" -- i.e. the environment's configuration at event time,
// not a live join against its current state).
// Caller must hold at least a write lock.
func (b *InMemoryBackend) appendEvent(ctx context.Context, region string, env *Environment, message, severity string) {
	b.appendEventAt(ctx, region, env, message, severity, time.Time{})
}

// appendEventAt records an event that stays hidden from DescribeEvents until visibleAt.
func (b *InMemoryBackend) appendEventAt(
	ctx context.Context,
	region string,
	env *Environment,
	message, severity string,
	visibleAt time.Time,
) {
	eventDate := nowISO8601()
	if !visibleAt.IsZero() {
		eventDate = visibleAt.UTC().Format("2006-01-02T15:04:05Z")
	}

	events := append(b.eventsSlice(region), &EventRecord{
		ApplicationName: env.ApplicationName,
		EnvironmentName: env.EnvironmentName,
		PlatformArn:     env.PlatformARN,
		TemplateName:    env.TemplateName,
		VersionLabel:    env.VersionLabel,
		EventDate:       eventDate,
		RequestID:       awsmeta.Get(ctx).RequestID,
		Message:         message,
		Severity:        severity,
		visibleAt:       visibleAt,
	})
	if len(events) > maxEventsPerRegion {
		events = events[len(events)-maxEventsPerRegion:]
	}
	b.events[region] = events
}

// DescribeEvents returns event records filtered by optional application and environment name.
// The most recent events are returned first (reverse insertion order).
func (b *InMemoryBackend) DescribeEvents(ctx context.Context, appName, envName string) []*EventRecord {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	events := b.eventsSliceRO(region)

	out := make([]*EventRecord, 0, len(events))

	for _, e := range slices.Backward(events) {
		if appName != "" && e.ApplicationName != appName {
			continue
		}

		if envName != "" && e.EnvironmentName != envName {
			continue
		}

		if !e.visibleAt.IsZero() && b.now().Before(e.visibleAt) {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	return out
}
