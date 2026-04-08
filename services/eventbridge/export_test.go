package eventbridge

import (
	"context"
	"time"
)

// MatchPatternForTest exposes the internal matchPattern function for external tests.
func MatchPatternForTest(pattern, event string) bool {
	return matchPattern(pattern, event)
}

// ScheduleForTest wraps a scheduleExpression for testing.
type ScheduleForTest struct {
	expr scheduleExpression
}

// ParseScheduleExpressionForTest exposes parseScheduleExpression for external tests.
func ParseScheduleExpressionForTest(expr string) (*ScheduleForTest, error) {
	s, err := parseScheduleExpression(expr)
	if err != nil {
		return nil, err
	}

	return &ScheduleForTest{expr: s}, nil
}

// NextAfterForTest exposes NextAfter for external tests.
func (s *ScheduleForTest) NextAfterForTest(t time.Time) time.Time {
	return s.expr.NextAfter(t)
}

// ProcessTickForTest exposes processTick so external tests can drive the
// scheduler synchronously and inspect lastFired cleanup behaviour.
func (s *Scheduler) ProcessTickForTest(ctx context.Context, tick time.Time, lastFired map[string]time.Time) {
	s.processTick(ctx, tick, lastFired)
}

// APIDestinationCount returns the number of API destinations in the backend.
func (b *InMemoryBackend) APIDestinationCount() int {
	b.mu.RLock("APIDestinationCount")
	defer b.mu.RUnlock()

	return len(b.apiDestinations)
}

// ArchiveCount returns the number of archives in the backend.
func (b *InMemoryBackend) ArchiveCount() int {
	b.mu.RLock("ArchiveCount")
	defer b.mu.RUnlock()

	return len(b.archives)
}

// ConnectionCount returns the number of connections in the backend.
func (b *InMemoryBackend) ConnectionCount() int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connections)
}

// EndpointCount returns the number of endpoints in the backend.
func (b *InMemoryBackend) EndpointCount() int {
	b.mu.RLock("EndpointCount")
	defer b.mu.RUnlock()

	return len(b.endpoints)
}

// EventSourceCount returns the number of event sources in the backend.
func (b *InMemoryBackend) EventSourceCount() int {
	b.mu.RLock("EventSourceCount")
	defer b.mu.RUnlock()

	return len(b.eventSources)
}

// ReplayCount returns the number of replays in the backend.
func (b *InMemoryBackend) ReplayCount() int {
	b.mu.RLock("ReplayCount")
	defer b.mu.RUnlock()

	return len(b.replays)
}

// PartnerSourceCount returns the number of partner event sources in the backend.
func (b *InMemoryBackend) PartnerSourceCount() int {
	b.mu.RLock("PartnerSourceCount")
	defer b.mu.RUnlock()

	return len(b.partnerSources)
}

// HandlerOpsLen returns the number of pre-built handler operations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}
