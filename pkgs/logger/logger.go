// Package logger is the project-wide slog wrapper. All logging goes through
// context.Context — services and handlers must NOT embed a *slog.Logger
// field. Use Load(ctx) to fetch the logger and WithService(ctx, name) at the
// service boundary so every record carries the same "service" attribute.
//
// Output format is fixed: text handler to stderr with RFC3339 timestamps and
// stable attribute ordering. This guarantees a single consistent format
// across the entire process; do not construct ad-hoc slog.New(...) instances
// in service code — call NewLogger or NewTestLogger here so format and
// handler options stay aligned.
package logger

import (
	"context"
	"log/slog"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
)

// Key is the typed context key under which the logger is stored. Exported so
// middleware that wants raw ctxval access can reuse it.
var Key = ctxval.NewKey[*slog.Logger]("logger") //nolint:gochecknoglobals // existing issue.

// Save stores logger in ctx and returns the child context.
func Save(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}

	return Key.Set(ctx, logger)
}

// Load returns the logger carried on ctx, or slog.Default() when none is set.
// Always returns non-nil so callers can chain methods without a guard.
func Load(ctx context.Context) *slog.Logger {
	if logger, ok := Key.Get(ctx); ok && logger != nil {
		return logger
	}

	return slog.Default()
}

// AddAttrs returns a child context whose logger has attrs pre-attached to
// every record. slog.Logger.With does not mutate the parent so this is safe
// for concurrent use across requests.
func AddAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	parent := Load(ctx)

	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}

	return Save(ctx, parent.With(args...))
}

// WithService returns a child context whose logger carries service=<name>.
// Call this once at the entry of every service handler so downstream records
// emitted via Load(ctx) are uniformly tagged. The service tag persists across
// AddAttrs calls because slog.Logger.With layers attributes.
func WithService(ctx context.Context, name string) context.Context {
	return AddAttrs(ctx, slog.String("service", name))
}

// handlerOptions returns the canonical handler options used by every logger
// constructed in this package. Keeping this in one place guarantees the same
// format everywhere: text output to stderr, fixed level, no AddSource.
func handlerOptions(level slog.Level) *slog.HandlerOptions {
	return &slog.HandlerOptions{
		Level: level,
	}
}

// NewLogger returns a logger writing text records to stderr at level.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, handlerOptions(level)))
}

// NewTestLogger returns a logger with debug level enabled, useful in tests.
func NewTestLogger() *slog.Logger {
	return NewLogger(slog.LevelDebug)
}
