package scheduler

import (
	"context"
	"time"
)

// ParseRateExpression exports parseRateExpression for testing.
func ParseRateExpression(expr string) (time.Duration, error) {
	return parseRateExpression(expr)
}

// ParseAtExpression exports parseAtExpression for testing.
func ParseAtExpression(expr string, loc *time.Location) (time.Time, error) {
	return parseAtExpression(expr, loc)
}

// CheckAndFireSchedules exports (r *Runner).checkAndFireSchedules for white-box testing
// without running a full goroutine.
func CheckAndFireSchedules(ctx context.Context, r *Runner, now time.Time) {
	r.checkAndFireSchedules(ctx, now)
}

// LastFiredAtLen returns the number of entries in the runner's lastFiredAt map.
// Intended for use in unit tests to verify memory-leak cleanup.
func LastFiredAtLen(r *Runner) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.lastFiredAt)
}

// ScheduleCount returns the total number of schedules in the backend across all regions.
func ScheduleCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduleCount")
	defer b.mu.RUnlock()

	return b.schedules.Len()
}

// ScheduleGroupCount returns the total number of schedule groups across all regions.
func ScheduleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduleGroupCount")
	defer b.mu.RUnlock()

	return b.scheduleGroups.Len()
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// CronCacheLen returns the number of entries in the runner's cronCache map.
// Intended for use in unit tests to verify cache eviction.
func CronCacheLen(r *Runner) int {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	return len(r.cronCache)
}

// LocCacheLen returns the number of entries in the runner's locCache map.
// Intended for use in unit tests to verify cache eviction.
func LocCacheLen(r *Runner) int {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	return len(r.locCache)
}

// IdempotencyCacheLen returns the number of entries in the handler's idempotency cache.
func IdempotencyCacheLen(h *Handler) int {
	return h.idempotency.Len()
}
