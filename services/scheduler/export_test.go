package scheduler

import (
	"context"
	"time"
)

// ParseRateExpression exports parseRateExpression for testing.
func ParseRateExpression(expr string) (time.Duration, error) {
	return parseRateExpression(expr)
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

// ScheduleCount returns the number of schedules in the backend.
func ScheduleCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduleCount")
	defer b.mu.RUnlock()

	return len(b.schedules)
}

// ScheduleGroupCount returns the number of schedule groups in the backend.
func ScheduleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduleGroupCount")
	defer b.mu.RUnlock()

	return len(b.scheduleGroups)
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
