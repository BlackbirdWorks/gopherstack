package telemetry

import (
	"math/rand/v2"
	"time"

	"github.com/labstack/echo/v5"
)

// LatencyMiddleware returns a middleware that injects a random sleep of [0, latencyMs)
// milliseconds before each request, simulating real-world network latency.
// This helps surface timeout bugs, race conditions, and missing retry logic.
// If latencyMs is zero or negative, the middleware is a no-op.
func LatencyMiddleware(latencyMs int) func(echo.HandlerFunc) echo.HandlerFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if latencyMs > 0 {
				//nolint:gosec // weak random is intentional — latency simulation is not security-sensitive
				n := rand.IntN(latencyMs)
				time.Sleep(time.Duration(n) * time.Millisecond)
			}

			return next(c)
		}
	}
}
