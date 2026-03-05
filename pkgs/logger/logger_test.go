package logger_test

import (
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogger_ContextSaveLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		saveLogger  bool
		wantDefault bool
	}{
		{
			name:       "save and load custom logger",
			saveLogger: true,
		},
		{
			name:        "load default when none saved",
			saveLogger:  false,
			wantDefault: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			if tt.saveLogger {
				testLogger := logger.NewTestLogger()
				ctx = logger.Save(ctx, testLogger)

				retrieved := logger.Load(ctx)

				require.NotNil(t, retrieved)
				assert.Equal(t, testLogger, retrieved)
			} else {
				retrieved := logger.Load(ctx)

				require.NotNil(t, retrieved)
				assert.Equal(t, slog.Default(), retrieved)
			}
		})
	}
}

func TestLogger_New(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func() *slog.Logger
		name             string
		wantDebugEnabled bool
		wantInfoEnabled  bool
	}{
		{
			name:             "test logger enables debug and info",
			setup:            logger.NewTestLogger,
			wantDebugEnabled: true,
			wantInfoEnabled:  true,
		},
		{
			name: "info logger disables debug but enables info",
			setup: func() *slog.Logger {
				return logger.NewLogger(slog.LevelInfo)
			},
			wantDebugEnabled: false,
			wantInfoEnabled:  true,
		},
		{
			name: "warn logger disables debug and info",
			setup: func() *slog.Logger {
				return logger.NewLogger(slog.LevelWarn)
			},
			wantDebugEnabled: false,
			wantInfoEnabled:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			l := tt.setup()

			require.NotNil(t, l)
			assert.Equal(t, tt.wantDebugEnabled, l.Enabled(ctx, slog.LevelDebug))
			assert.Equal(t, tt.wantInfoEnabled, l.Enabled(ctx, slog.LevelInfo))
		})
	}
}

func TestLogger_AddAttrs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialLogger *slog.Logger
		attrs         []slog.Attr
	}{
		{
			name:          "adds single attr without mutating parent",
			initialLogger: logger.NewTestLogger(),
			attrs:         []slog.Attr{slog.String("service", "DynamoDB")},
		},
		{
			name:          "adds multiple attrs",
			initialLogger: logger.NewTestLogger(),
			attrs: []slog.Attr{
				slog.String("service", "S3"),
				slog.String("request_id", "abc-123"),
			},
		},
		{
			name:  "works when no logger saved in ctx (uses default)",
			attrs: []slog.Attr{slog.String("key", "val")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.initialLogger != nil {
				ctx = logger.Save(ctx, tt.initialLogger)
			}

			parent := logger.Load(ctx)

			enrichedCtx := logger.AddAttrs(ctx, tt.attrs...)
			child := logger.Load(enrichedCtx)

			// Child must be a distinct pointer so the parent is not mutated.
			require.NotNil(t, child)
			assert.NotSame(t, parent, child)

			// Parent logger in original ctx must be unchanged.
			assert.Same(t, parent, logger.Load(ctx))
		})
	}
}
