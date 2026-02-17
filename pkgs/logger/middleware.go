package logger

import (
	"log/slog"
	"net/http"
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
