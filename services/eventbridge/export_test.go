package eventbridge

import (
	"context"
	"fmt"
	"time"
)

// RegionContextKeyForTest exports the regionContextKey type for external tests.
type RegionContextKeyForTest = regionContextKey

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

// APIDestinationCount returns the number of API destinations in the backend (default region).
func (b *InMemoryBackend) APIDestinationCount() int {
	b.mu.RLock("APIDestinationCount")
	defer b.mu.RUnlock()

	return len(b.apiDestinationsStore(b.region))
}

// ArchiveCount returns the number of archives in the backend (default region).
func (b *InMemoryBackend) ArchiveCount() int {
	b.mu.RLock("ArchiveCount")
	defer b.mu.RUnlock()

	return len(b.archivesStore(b.region))
}

// ConnectionCount returns the number of connections in the backend (default region).
func (b *InMemoryBackend) ConnectionCount() int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connectionsStore(b.region))
}

// EndpointCount returns the number of endpoints in the backend (default region).
func (b *InMemoryBackend) EndpointCount() int {
	b.mu.RLock("EndpointCount")
	defer b.mu.RUnlock()

	return len(b.endpointsStore(b.region))
}

// EventSourceCount returns the number of event sources in the backend (default region).
func (b *InMemoryBackend) EventSourceCount() int {
	b.mu.RLock("EventSourceCount")
	defer b.mu.RUnlock()

	return len(b.eventSourcesStore(b.region))
}

// ReplayCount returns the number of replays in the backend (default region).
func (b *InMemoryBackend) ReplayCount() int {
	b.mu.RLock("ReplayCount")
	defer b.mu.RUnlock()

	return len(b.replaysStore(b.region))
}

// PartnerSourceCount returns the number of partner event sources in the backend (default region).
func (b *InMemoryBackend) PartnerSourceCount() int {
	b.mu.RLock("PartnerSourceCount")
	defer b.mu.RUnlock()

	return len(b.partnerSourcesStore(b.region))
}

// HandlerOpsLen returns the number of pre-built handler operations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// PatternCacheSize returns the number of compiled patterns in cache.
func (b *InMemoryBackend) PatternCacheSize() int {
	size := 0
	b.patternCache.Range(func(_, _ any) bool {
		size++

		return true
	})

	return size
}

// SetJanitorNow overrides the clock function used by ArchiveJanitor for testing.
func (j *ArchiveJanitor) SetNow(now time.Time) {
	j.now = func() time.Time { return now }
}

// ArchivedEventCount returns the number of archived events for a given archive name (default region).
func (b *InMemoryBackend) ArchivedEventCount(archiveName string) int {
	b.mu.RLock("ArchivedEventCount")
	defer b.mu.RUnlock()

	return len(b.archivedEventsStore(b.region)[archiveName])
}

// SetArchiveCreationTimeForTest overrides an archive creation time.
func (b *InMemoryBackend) SetArchiveCreationTimeForTest(name string, creationTime time.Time) error {
	b.mu.Lock("SetArchiveCreationTimeForTest")
	defer b.mu.Unlock()

	store := b.archivesStore(b.region)
	archive, exists := store[name]
	if !exists {
		return fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	archive.CreationTime = creationTime

	return nil
}

// EventLogLen returns the number of entries in the in-memory event log.
func (b *InMemoryBackend) EventLogLen() int {
	b.mu.RLock("EventLogLen")
	defer b.mu.RUnlock()

	return len(b.eventLog)
}

// MaxEventLogSizeForTest exposes the in-memory event log size cap.
const MaxEventLogSizeForTest = maxEventLogSize

// TargetsByARNCount returns how many targetKeys are indexed for the given region+ARN.
func (b *InMemoryBackend) TargetsByARNCount(region, arn string) int {
	b.mu.RLock("TargetsByARNCount")
	defer b.mu.RUnlock()

	if rm := b.targetsByARN[region]; rm != nil {
		return len(rm[arn])
	}

	return 0
}

// ARNIndexConsistent verifies targetsByARN matches the canonical targets map.
// Returns false and a description if not.
func (b *InMemoryBackend) ARNIndexConsistent() (bool, string) {
	b.mu.RLock("ARNIndexConsistent")
	defer b.mu.RUnlock()

	for region, regionTargets := range b.targets {
		for targetKey, tMap := range regionTargets {
			for _, t := range tMap {
				if rm := b.targetsByARN[region]; rm == nil {
					return false, fmt.Sprintf("targetsByARN[%s] is nil but canonical has entries", region)
				} else if _, ok := rm[t.Arn][targetKey]; !ok {
					return false, fmt.Sprintf("targetKey %s ARN %s missing from targetsByARN[%s]", targetKey, t.Arn, region)
				}
			}
		}
	}

	return true, ""
}
