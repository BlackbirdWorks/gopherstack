package memorydb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// AddEvent appends an event to the backend event log (used internally for seeding).
// Events are capped at maxEvents; oldest entries are dropped when the cap is reached.
func (b *InMemoryBackend) AddEvent(ev *Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.appendEventLocked(b.defaultRegion, ev)
}

// appendEventLocked appends an event without acquiring the lock (caller must hold b.mu).
func (b *InMemoryBackend) appendEventLocked(region string, ev *Event) {
	b.events[region] = append(b.events[region], ev)

	if len(b.events[region]) > maxEvents {
		trimmed := make([]*Event, maxEvents)
		copy(trimmed, b.events[region][len(b.events[region])-maxEvents:])
		b.events[region] = trimmed
	}
}

// DescribeEvents returns events, optionally filtered by source name and type.
func (b *InMemoryBackend) DescribeEvents(_ context.Context, req *describeEventsRequest) ([]*Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	startTime, err := resolveEventStartTime(req)
	if err != nil {
		return nil, err
	}

	endTime, err := parseEpochRequestTime(req.EndTime)
	if err != nil {
		return nil, err
	}

	var result []*Event

	for _, evs := range b.events {
		for _, ev := range evs {
			if eventMatchesFilter(ev, req, startTime, endTime) {
				result = append(result, cloneEvent(ev))
			}
		}
	}

	// Deterministic order: map iteration over b.events (keyed by region) is
	// randomized, and pagination requires a stable ordering across calls.
	sort.Slice(result, func(i, j int) bool {
		if !result[i].Date.Equal(result[j].Date) {
			return result[i].Date.Before(result[j].Date)
		}

		if result[i].SourceName != result[j].SourceName {
			return result[i].SourceName < result[j].SourceName
		}

		return result[i].Message < result[j].Message
	})

	return result, nil
}

// parseEpochRequestTime parses a describeEventsRequest StartTime/EndTime
// field. Real aws-sdk-go-v2 clients serialize these TStamp shapes as a JSON
// number of epoch seconds (awsjson1.1), so n arrives as json.Number, not an
// RFC3339 string. Returns nil, nil when n is the empty (unset) value.
func parseEpochRequestTime(n json.Number) (*time.Time, error) {
	if n == "" {
		return nil, nil //nolint:nilnil // absent filter is a valid, distinct state from "parse failed"
	}

	f, err := n.Float64()
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q: %w", n, ErrValidation)
	}

	sec := int64(f)
	nsec := int64((f - float64(sec)) * float64(time.Second))
	t := time.Unix(sec, nsec).UTC()

	return &t, nil
}

func resolveEventStartTime(req *describeEventsRequest) (*time.Time, error) {
	startTime, err := parseEpochRequestTime(req.StartTime)
	if err != nil {
		return nil, err
	}

	if startTime != nil {
		return startTime, nil
	}

	if req.Duration != nil {
		t := time.Now().Add(-time.Duration(*req.Duration) * time.Minute)

		return &t, nil
	}

	return nil, nil //nolint:nilnil // no start filter is a valid state
}

func eventMatchesFilter(ev *Event, req *describeEventsRequest, startTime, endTime *time.Time) bool {
	if req.SourceName != "" && ev.SourceName != req.SourceName {
		return false
	}

	if req.SourceType != "" && ev.SourceType != req.SourceType {
		return false
	}

	if startTime != nil && ev.Date.Before(*startTime) {
		return false
	}

	if endTime != nil && ev.Date.After(*endTime) {
		return false
	}

	return true
}

// cloneEvent returns a shallow copy of an Event.
func cloneEvent(e *Event) *Event {
	cp := *e

	return &cp
}

// -- MultiRegionCluster operations ----------------------------------------------
