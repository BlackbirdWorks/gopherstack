package telemetry

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// memStatsRefreshInterval throttles runtime.ReadMemStats (~5% of request CPU when per-request).
const memStatsRefreshInterval = 200 * time.Millisecond

//nolint:gochecknoglobals // process-wide cache refreshed on a time budget, not per request
var (
	memStatsCached  atomic.Value // string
	memStatsNextRun atomic.Int64 // unix nano
)

// MemoryStatsMiddleware injects the X-Gopherstack-Memory-Stats header into all responses.
// The header contains Alloc, TotalAlloc, Sys, and NumGC stats from runtime.MemStats.
// This allows long-term monitoring of the Gopherstack container's memory health.
func MemoryStatsMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		err := next(c)

		c.Response().Header().Set("X-Gopherstack-Memory-Stats", currentMemStats())

		return err
	}
}

// currentMemStats returns the cached stats, refreshed by the CAS winner at most once per interval.
func currentMemStats() string {
	now := time.Now().UnixNano()
	deadline := memStatsNextRun.Load()

	if now < deadline {
		if stats, ok := memStatsCached.Load().(string); ok {
			return stats
		}
	}

	if !memStatsNextRun.CompareAndSwap(deadline, now+int64(memStatsRefreshInterval)) {
		if stats, ok := memStatsCached.Load().(string); ok {
			return stats
		}
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	stats := fmt.Sprintf("Alloc=%d,TotalAlloc=%d,Sys=%d,NumGC=%d", ms.Alloc, ms.TotalAlloc, ms.Sys, ms.NumGC)
	memStatsCached.Store(stats)

	return stats
}
