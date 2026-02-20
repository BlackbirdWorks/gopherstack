package telemetry

import (
	"log/slog"
	"time"

	"github.com/labstack/echo/v5"
)

// ObservabilityObserver extracts metrics labels from an Echo request.
type ObservabilityObserver interface {
	// ExtractOperation returns the operation name.
	ExtractOperation(c *echo.Context) string

	// ExtractResource returns the resource identifier.
	ExtractResource(c *echo.Context) string
}

// WrapEchoHandler wraps an Echo handler with automated metrics and logging.
// The wrapper extracts operation and resource names via the observer, times
// the handler execution, records metrics, and logs the result.
//
// This eliminates boilerplate from service handlers—they focus on business
// logic while observability is handled automatically.
func WrapEchoHandler(
	serviceName string,
	handler echo.HandlerFunc,
	observer ObservabilityObserver,
	logger *slog.Logger,
) echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Extract metrics labels from request
		operation := observer.ExtractOperation(c)
		resource := observer.ExtractResource(c)

		// Log request start
		logger.Debug(
			"operation started",
			"service", serviceName,
			"operation", operation,
			"resource", resource,
			"method", c.Request().Method,
			"path", c.Request().URL.Path,
		)

		// Time the operation
		start := time.Now()

		// Call the handler
		err := handler(c)

		// Re-extract operation in case the handler refined it (common for S3)
		if operation == "Unknown" || operation == "unknown" {
			operation = observer.ExtractOperation(c)
		}

		duration := time.Since(start)
		durationSeconds := duration.Seconds()

		// Determine status from response code
		status := "success"
		echoResp, ok := c.Response().(*echo.Response)
		if err != nil || (ok && echoResp.Status >= 400) || (!ok && c.Response().Header().Get("X-Error") != "") {
			status = "error"
		}

		// Record metrics
		metricsLabel := serviceName + "::" + operation
		RecordOperation(metricsLabel, resource, durationSeconds, status)

		// Log result
		if err != nil {
			logger.Error(
				"operation failed",
				"service", serviceName,
				"operation", operation,
				"resource", resource,
				"duration_seconds", durationSeconds,
				"error", err,
			)

			return err
		}

		if ok && echoResp.Status >= 400 {
			logger.Warn(
				"operation completed with error status",
				"service", serviceName,
				"operation", operation,
				"resource", resource,
				"status_code", echoResp.Status,
				"duration_seconds", durationSeconds,
			)
		} else {
			logger.Debug(
				"operation completed",
				"service", serviceName,
				"operation", operation,
				"resource", resource,
				"status_code", map[bool]int{true: echoResp.Status}[ok],
				"duration_seconds", durationSeconds,
			)
		}

		return nil // Don't return the handler error again
	}
}
