package elasticache

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxEvents = 1000

// CacheEvent represents a recorded ElastiCache operation event.
type CacheEvent struct {
	Date             time.Time `json:"date"`
	SourceIdentifier string    `json:"sourceIdentifier"`
	SourceType       string    `json:"sourceType"`
	Message          string    `json:"message"`
}

// eventRing is a fixed-capacity circular ring buffer for CacheEvents.
// It stores up to size events, overwriting the oldest when full.
// Reads allocate and return events in insertion order.
type eventRing struct {
	buf  []CacheEvent
	head int // index of oldest entry
	n    int // number of valid entries
	size int
}

func newEventRing(size int) *eventRing {
	return &eventRing{buf: make([]CacheEvent, size), size: size}
}

// push appends e, overwriting the oldest entry when the ring is full.
func (r *eventRing) push(e CacheEvent) {
	if r.n < r.size {
		r.buf[(r.head+r.n)%r.size] = e
		r.n++
	} else {
		// Full: overwrite oldest.
		r.buf[r.head] = e
		r.head = (r.head + 1) % r.size
	}
}

// all returns a snapshot of all events in insertion order.
func (r *eventRing) all() []CacheEvent {
	out := make([]CacheEvent, r.n)
	for i := range r.n {
		out[i] = r.buf[(r.head+i)%r.size]
	}

	return out
}

// reset clears the ring without reallocating the backing buffer.
func (r *eventRing) reset() {
	r.head = 0
	r.n = 0
}

// marshalJSON exports events in insertion order for persistence.
func (r *eventRing) marshalJSON() []CacheEvent {
	return r.all()
}

// restoreFromSlice loads previously-persisted events back into the ring.
func (r *eventRing) restoreFromSlice(events []CacheEvent) {
	r.reset()
	for _, e := range events {
		r.push(e)
	}
}

// appendEventLocked records a new event. Must be called with b.mu write-locked.
func (b *InMemoryBackend) appendEventLocked(sourceIdentifier, sourceType, message string) {
	b.events.push(CacheEvent{
		Date:             b.now(),
		SourceIdentifier: sourceIdentifier,
		SourceType:       sourceType,
		Message:          message,
	})
}

// defaultEventsWindow is DescribeEvents's documented default lookback
// (api_op_DescribeEvents.go: "By default, only the events occurring within
// the last hour are returned").
const defaultEventsWindow = time.Hour

// DescribeEvents returns a paginated list of recorded events, optionally filtered by source and time.
func (b *InMemoryBackend) DescribeEvents(
	_ context.Context,
	sourceIdentifier, sourceType, marker string,
	startTime, endTime time.Time,
	duration, maxRecords int,
) (page.Page[CacheEvent], error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	// Duration is documented in minutes (api_op_DescribeEvents.go: "The
	// number of minutes worth of events to retrieve"), not seconds. Absent
	// both Duration and StartTime, only the last hour is returned by
	// default -- omitting every time bound must narrow the window, not
	// return every event ever recorded.
	effectiveStart := startTime

	switch {
	case duration > 0:
		effectiveStart = b.now().Add(-time.Duration(duration) * time.Minute)
	case effectiveStart.IsZero():
		effectiveStart = b.now().Add(-defaultEventsWindow)
	}

	all := b.events.all()
	out := make([]CacheEvent, 0, len(all))
	for _, e := range all {
		if sourceIdentifier != "" && e.SourceIdentifier != sourceIdentifier {
			continue
		}
		if sourceType != "" && e.SourceType != sourceType {
			continue
		}
		if e.Date.Before(effectiveStart) {
			continue
		}
		if !endTime.IsZero() && e.Date.After(endTime) {
			continue
		}
		out = append(out, e)
	}

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}
