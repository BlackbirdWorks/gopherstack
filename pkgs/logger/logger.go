package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const loggerKey contextKey = "logger"

// Save stores a logger in the context.
func Save(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// Load retrieves a logger from the context.
// If no logger is found, it returns [slog.Default].
func Load(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}

	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok && logger != nil {
		return logger
	}

	return slog.Default()
}

// AddAttrs returns a new context containing a child logger derived from the
// one already stored in ctx. The child logger has the given attributes
// pre-attached to every record it emits.
//
// Because [slog.Logger.With] returns a brand-new [*slog.Logger] without mutating
// the parent, it is safe to call AddAttrs concurrently from many goroutines
// without risking cross-request attribute bleed.
func AddAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	parent := Load(ctx)

	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}

	return Save(ctx, parent.With(args...))
}

// NewTestLogger creates a logger suitable for testing with debug level enabled.
func NewTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// NewLogger creates a logger with the specified level.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}
