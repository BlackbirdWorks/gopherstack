package telemetry

import (
	"fmt"
	"runtime"

	"github.com/labstack/echo/v5"
)

// MemoryStatsMiddleware injects the X-Gopherstack-Memory-Stats header into all responses.
// The header contains Alloc, TotalAlloc, Sys, and NumGC stats from runtime.MemStats.
// This allows long-term monitoring of the Gopherstack container's memory health.
func MemoryStatsMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		err := next(c)

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		// Format: Alloc={Alloc},TotalAlloc={TotalAlloc},Sys={Sys},NumGC={NumGC}
		stats := fmt.Sprintf("Alloc=%d,TotalAlloc=%d,Sys=%d,NumGC=%d",
			ms.Alloc, ms.TotalAlloc, ms.Sys, ms.NumGC)

		c.Response().Header().Set("X-Gopherstack-Memory-Stats", stats)

		return err
	}
}
