package logger_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/stretchr/testify/assert"
)

func TestSaveLoad(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testLogger := logger.NewTestLogger()

	// Save logger to context
	ctx = logger.Save(ctx, testLogger)

	// Load logger from context
	retrieved := logger.Load(ctx)

	assert.Equal(t, testLogger, retrieved)
}

func TestLoadDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Load without saving should return default
	retrieved := logger.Load(ctx)

	assert.NotNil(t, retrieved)
	assert.Equal(t, slog.Default(), retrieved)
}

func TestNewTestLogger(t *testing.T) {
	t.Parallel()

	testLogger := logger.NewTestLogger()

	assert.NotNil(t, testLogger)
	assert.True(t, testLogger.Enabled(context.Background(), slog.LevelDebug))
}

func TestNewLogger(t *testing.T) {
	t.Parallel()

	infoLogger := logger.NewLogger(slog.LevelInfo)

	assert.NotNil(t, infoLogger)
	assert.False(t, infoLogger.Enabled(context.Background(), slog.LevelDebug))
	assert.True(t, infoLogger.Enabled(context.Background(), slog.LevelInfo))
}
