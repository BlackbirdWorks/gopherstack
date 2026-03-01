package logger_test

import (
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/stretchr/testify/assert"
)

func TestLogger_SaveLoad(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	testLogger := logger.NewTestLogger()

	ctx = logger.Save(ctx, testLogger)

	retrieved := logger.Load(ctx)

	assert.Equal(t, testLogger, retrieved)
}

func TestLogger_LoadDefault(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	retrieved := logger.Load(ctx)

	assert.NotNil(t, retrieved)
	assert.Equal(t, slog.Default(), retrieved)
}

func TestLogger_NewTestLogger(t *testing.T) {
	t.Parallel()

	testLogger := logger.NewTestLogger()

	assert.NotNil(t, testLogger)
	assert.True(t, testLogger.Enabled(t.Context(), slog.LevelDebug))
}

func TestLogger_NewLogger(t *testing.T) {
	t.Parallel()

	infoLogger := logger.NewLogger(slog.LevelInfo)

	assert.NotNil(t, infoLogger)
	assert.False(t, infoLogger.Enabled(t.Context(), slog.LevelDebug))
	assert.True(t, infoLogger.Enabled(t.Context(), slog.LevelInfo))
}
