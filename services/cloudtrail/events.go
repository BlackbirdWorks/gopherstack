package cloudtrail

import (
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// RecordEvent stores a management/data event so it can later be returned by
// LookupEvents. The event is assigned an EventID and EventTime if not already set.
func (b *InMemoryBackend) RecordEvent(ev Event) {
	b.mu.Lock("RecordEvent")
	defer b.mu.Unlock()

	if ev.EventID == "" {
		ev.EventID = uuid.NewString()
	}

	if ev.EventTime.IsZero() {
		ev.EventTime = time.Now().UTC()
	}

	b.events = append(b.events, ev)
}

// lookupAttrMatch reports whether an event matches a single lookup attribute.
func lookupAttrMatch(ev Event, attr LookupAttribute) bool {
	switch attr.AttributeKey {
	case "EventId":
		return ev.EventID == attr.AttributeValue
	case "EventName":
		return ev.EventName == attr.AttributeValue
	case "EventSource":
		return ev.EventSource == attr.AttributeValue
	case "Username":
		return ev.Username == attr.AttributeValue
	case "ReadOnly":
		return ev.ReadOnly == attr.AttributeValue
	case "AccessKeyId":
		return ev.AccessKeyID == attr.AttributeValue
	case "ResourceName":
		for _, r := range ev.Resources {
			if r.ResourceName == attr.AttributeValue {
				return true
			}
		}

		return false
	case "ResourceType":
		for _, r := range ev.Resources {
			if r.ResourceType == attr.AttributeValue {
				return true
			}
		}

		return false
	default:
		return false
	}
}

// eventMatchesFilters reports whether an event passes the time range and all
// lookup attributes (AWS ANDs multiple attributes together).
func eventMatchesFilters(ev Event, input LookupEventsInput) bool {
	if input.StartTime != nil && ev.EventTime.Before(*input.StartTime) {
		return false
	}

	if input.EndTime != nil && ev.EventTime.After(*input.EndTime) {
		return false
	}

	for _, attr := range input.LookupAttributes {
		if !lookupAttrMatch(ev, attr) {
			return false
		}
	}

	return true
}

// LookupEvents returns recorded events matching the given filters. Events are
// returned newest-first (matching AWS) and honor StartTime/EndTime, the lookup
// attributes (ANDed together), MaxResults, and NextToken pagination.
func (b *InMemoryBackend) LookupEvents(input LookupEventsInput) LookupEventsOutput {
	b.mu.RLock("LookupEvents")
	defer b.mu.RUnlock()

	matched := make([]Event, 0, len(b.events))

	for _, ev := range b.events {
		if eventMatchesFilters(ev, input) {
			matched = append(matched, ev)
		}
	}

	// Newest-first ordering, as the AWS API returns.
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].EventTime.After(matched[j].EventTime)
	})

	// Decode the NextToken (offset into the matched slice).
	start := 0

	if input.NextToken != "" {
		if n, err := strconv.Atoi(input.NextToken); err == nil && n >= 0 && n <= len(matched) {
			start = n
		}
	}

	limit := int(input.MaxResults)
	if limit <= 0 || limit > 50 {
		limit = 50
	}

	end := start + limit

	var nextToken string

	if end < len(matched) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(matched)
	}

	page := make([]Event, 0, end-start)
	if start < len(matched) {
		page = append(page, matched[start:end]...)
	}

	return LookupEventsOutput{Events: page, NextToken: nextToken}
}
