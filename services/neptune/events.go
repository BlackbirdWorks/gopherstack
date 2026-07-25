package neptune

import (
	"context"
	"strings"
	"time"
)

// This file backs the "DescribeEvents always returns an empty list" gap
// identified in PARITY.md: there was no event log backing this backend at
// all, so DescribeEvents could never report anything regardless of what a
// caller had actually done. recordEvent is now called from the key
// cluster/instance/snapshot lifecycle mutators (create/delete/start/stop/
// failover) while they already hold the backend write lock, appending a real
// entry DescribeEvents can filter and return.

// maxEventsLogPerRegion bounds eventsLog growth in a long-lived backend,
// matching the bounded-history spirit of AWS's own event retention (AWS
// retains ~2 weeks; this backend retains the most recent N entries instead of
// tracking wall-clock age).
const maxEventsLogPerRegion = 500

// Neptune event source types, matching types.SourceType.
const (
	sourceTypeDBCluster         = "db-cluster"
	sourceTypeDBInstance        = "db-instance"
	sourceTypeDBClusterSnapshot = "db-cluster-snapshot"
)

// recordEvent appends an event to region's log, trimming the oldest entries
// once maxEventsLogPerRegion is exceeded. Callers must already hold the
// backend write lock (every call site is inside an existing Lock/defer
// Unlock section of a mutating op), so this does not lock itself.
func (b *InMemoryBackend) recordEvent(
	region, sourceID, sourceType, message string, categories ...string,
) {
	cats := make([]string, len(categories))
	copy(cats, categories)
	log := b.eventsLog[region]
	log = append(log, Event{
		SourceIdentifier: sourceID,
		SourceType:       sourceType,
		Message:          message,
		Date:             nowISO8601(),
		EventCategories:  cats,
	})
	if len(log) > maxEventsLogPerRegion {
		log = log[len(log)-maxEventsLogPerRegion:]
	}
	b.eventsLog[region] = log
}

// EventsFilter holds the filter values DescribeEvents accepts.
type EventsFilter struct {
	SourceIdentifier string
	SourceType       string
	StartTime        string
	EndTime          string
	EventCategories  []string
	Duration         int
}

// DescribeEvents returns the account activity events recorded for the
// request's region, filtered per filter. Matches AWS's default lookback
// window (Duration minutes, default 60) when no explicit StartTime is given.
func (b *InMemoryBackend) DescribeEvents(ctx context.Context, filter EventsFilter) []Event {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()
	start, end := resolveEventsWindow(filter)
	result := make([]Event, 0, len(b.eventsLog[region]))
	for _, e := range b.eventsLog[region] {
		if !eventMatches(e, filter, start, end) {
			continue
		}
		cp := e
		cp.EventCategories = make([]string, len(e.EventCategories))
		copy(cp.EventCategories, e.EventCategories)
		result = append(result, cp)
	}

	return result
}

// resolveEventsWindow computes the [start, end) time window DescribeEvents
// filters against, mirroring AWS's precedence: an explicit StartTime wins
// over Duration; Duration falls back to a 60-minute lookback from now when
// neither StartTime nor Duration is given. A zero time.Time means "no bound".
func resolveEventsWindow(filter EventsFilter) (time.Time, time.Time) {
	const defaultDurationMinutes = 60
	var end time.Time
	if filter.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, filter.EndTime); err == nil {
			end = t
		}
	}
	if filter.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, filter.StartTime); err == nil {
			return t, end
		}
	}
	duration := filter.Duration
	if duration <= 0 {
		duration = defaultDurationMinutes
	}

	return time.Now().UTC().Add(-time.Duration(duration) * time.Minute), end
}

// eventMatches reports whether e satisfies filter's source/category/time
// constraints.
func eventMatches(e Event, filter EventsFilter, start, end time.Time) bool {
	if filter.SourceIdentifier != "" && e.SourceIdentifier != filter.SourceIdentifier {
		return false
	}
	if filter.SourceType != "" && e.SourceType != filter.SourceType {
		return false
	}
	if len(filter.EventCategories) > 0 && !anyCategoryMatches(e.EventCategories, filter.EventCategories) {
		return false
	}
	eventTime, err := time.Parse(time.RFC3339, e.Date)
	if err != nil {
		return true
	}
	if !start.IsZero() && eventTime.Before(start) {
		return false
	}
	if !end.IsZero() && eventTime.After(end) {
		return false
	}

	return true
}

// anyCategoryMatches reports whether have and want share at least one
// category (case-insensitively, matching AWS's category name handling).
func anyCategoryMatches(have, want []string) bool {
	for _, w := range want {
		for _, h := range have {
			if strings.EqualFold(h, w) {
				return true
			}
		}
	}

	return false
}
