package logger

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v5"
)

// Middleware creates an HTTP middleware that injects a logger into the request context.
func Middleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := Save(r.Context(), logger)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// EchoMiddleware creates an Echo middleware that injects a logger into the request context.
func EchoMiddleware(logger *slog.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := Save(c.Request().Context(), logger)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}
