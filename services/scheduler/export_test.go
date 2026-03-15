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
