package logger_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "SaveLoad", run: func(t *testing.T) {
			ctx := context.Background()
			testLogger := logger.NewTestLogger()

			// Save logger to context
			ctx = logger.Save(ctx, testLogger)

			// Load logger from context
			retrieved := logger.Load(ctx)

			assert.Equal(t, testLogger, retrieved)
		}},
		{name: "LoadDefault", run: func(t *testing.T) {
			ctx := context.Background()

			// Load without saving should return default
			retrieved := logger.Load(ctx)

			assert.NotNil(t, retrieved)
			assert.Equal(t, slog.Default(), retrieved)
		}},
		{name: "NewTestLogger", run: func(t *testing.T) {
			testLogger := logger.NewTestLogger()

			assert.NotNil(t, testLogger)
			assert.True(t, testLogger.Enabled(context.Background(), slog.LevelDebug))
		}},
		{name: "NewLogger", run: func(t *testing.T) {
			infoLogger := logger.NewLogger(slog.LevelInfo)

			assert.NotNil(t, infoLogger)
			assert.False(t, infoLogger.Enabled(context.Background(), slog.LevelDebug))
			assert.True(t, infoLogger.Enabled(context.Background(), slog.LevelInfo))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
