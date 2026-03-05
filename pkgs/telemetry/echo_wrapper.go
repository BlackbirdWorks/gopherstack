package telemetry

import (
	"log/slog"
	"time"

	"github.com/labstack/echo/v5"

	pkglogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
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
// Per request, it enriches the request context logger with a "service"
// attribute so that all downstream log lines are automatically tagged.
// The logger is pulled from the request context via [pkglogger.Load]; callers
// are expected to inject a logger into the context before requests arrive
// (e.g. via [pkglogger.EchoMiddleware]).
//
// This eliminates boilerplate from service handlers—they focus on business
// logic while observability is handled automatically.
func WrapEchoHandler(
	serviceName string,
	handler echo.HandlerFunc,
	observer ObservabilityObserver,
) echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Enrich the request-scoped context logger with the service name so
		// that every log line emitted during this request is tagged with it.
		// AddAttrs creates a *new* logger derived from the one already in ctx,
		// leaving the shared root logger untouched.
		reqCtx := pkglogger.AddAttrs(
			c.Request().Context(),
			slog.String("service", serviceName),
		)
		c.SetRequest(c.Request().WithContext(reqCtx))

		log := pkglogger.Load(reqCtx)

		// Extract metrics labels from request
		operation := observer.ExtractOperation(c)
		resource := observer.ExtractResource(c)

		// Log request start
		log.DebugContext(
			reqCtx,
			"operation started",
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
			log.ErrorContext(
				reqCtx,
				"operation failed",
				"operation", operation,
				"resource", resource,
				"duration_seconds", durationSeconds,
				"error", err,
			)

			return err
		}

		if ok && echoResp.Status >= 400 {
			log.WarnContext(
				reqCtx,
				"operation completed with error status",
				"operation", operation,
				"resource", resource,
				"status_code", echoResp.Status,
				"duration_seconds", durationSeconds,
			)
		} else {
			log.DebugContext(
				reqCtx,
				"operation completed",
				"operation", operation,
				"resource", resource,
				"status_code", map[bool]int{true: echoResp.Status}[ok],
				"duration_seconds", durationSeconds,
			)
		}

		return nil // Don't return the handler error again
	}
}
