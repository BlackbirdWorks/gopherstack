package dax

import (
	"strconv"
	"time"
)

// eventMatches returns true if the event matches the given filters.
func eventMatches(ev *Event, sourceName, sourceType string, startTime, endTime *time.Time) bool {
	if sourceName != "" && ev.SourceName != sourceName {
		return false
	}

	if sourceType != "" && ev.SourceType != sourceType {
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

// DescribeEvents returns events filtered by source and time range.
func (b *InMemoryBackend) DescribeEvents(
	sourceName string,
	sourceType string,
	startTime *time.Time,
	endTime *time.Time,
	maxResults int,
	nextToken string,
) ([]*Event, string, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxClustersDefault
	}

	var filtered []*Event

	for _, ev := range b.events {
		if !eventMatches(ev, sourceName, sourceType, startTime, endTime) {
			continue
		}

		cp := *ev
		filtered = append(filtered, &cp)
	}

	// Apply pagination via token = index as string.
	start := 0

	if nextToken != "" {
		idx, err := strconv.Atoi(nextToken)
		if err == nil && idx >= 0 && idx < len(filtered) {
			start = idx
		}
	}

	if start >= len(filtered) {
		return []*Event{}, "", nil
	}

	end := start + maxResults
	newNextToken := ""

	if end < len(filtered) {
		newNextToken = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}

	return filtered[start:end], newNextToken, nil
}

// emitEventLocked appends an event to the ring buffer. Must be called with b.mu held for write.
func (b *InMemoryBackend) emitEventLocked(sourceName, sourceType, message string) {
	ev := &Event{
		Date:       time.Now().UTC(),
		SourceName: sourceName,
		SourceType: sourceType,
		Message:    message,
	}

	b.events = append(b.events, ev)

	if len(b.events) > maxEventsPerBuffer {
		b.events = b.events[len(b.events)-maxEventsPerBuffer:]
	}
}
