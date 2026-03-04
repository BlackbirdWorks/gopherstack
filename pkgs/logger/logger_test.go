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
		name             string
		setup            func() *slog.Logger
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
