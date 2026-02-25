package eventbridge_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_FiresRateRule(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	backend.SetLogger(log)

	// Create a rule with a short rate expression (fires every second).
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:               "sched-rule",
		ScheduleExpression: "rate(1 second)",
		State:              "ENABLED",
	})
	require.NoError(t, err)

	// Use a very short tick interval to trigger firing quickly in tests.
	tickInterval := 50 * time.Millisecond
	scheduler := eventbridge.NewScheduler(backend, log, tickInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go scheduler.Run(ctx)

	// Wait for at least one scheduled event to appear in the event log.
	require.Eventually(t, func() bool {
		events := backend.GetEventLog()

		for _, entry := range events {
			if entry.Source == "aws.events" {
				return true
			}
		}

		return false
	}, 5*time.Second, 100*time.Millisecond, "expected at least one scheduled event to be fired")
}

func TestScheduler_SkipsDisabledRule(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	backend.SetLogger(log)

	// Create a DISABLED rule.
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:               "disabled-sched-rule",
		ScheduleExpression: "rate(1 minute)",
		State:              "DISABLED",
	})
	require.NoError(t, err)

	tickInterval := 50 * time.Millisecond
	scheduler := eventbridge.NewScheduler(backend, log, tickInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go scheduler.Run(ctx)

	// Disabled rules should not fire events.
	time.Sleep(300 * time.Millisecond)
	events := backend.GetEventLog()
	for _, e := range events {
		assert.NotEqual(t, "aws.events", e.Source, "disabled rule should not fire events")
	}
}

func TestScheduler_InvalidExpression(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	backend.SetLogger(log)

	// Create a rule with an invalid schedule expression.
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:               "bad-sched-rule",
		ScheduleExpression: "invalid(expression)",
		State:              "ENABLED",
	})
	require.NoError(t, err)

	tickInterval := 50 * time.Millisecond
	scheduler := eventbridge.NewScheduler(backend, log, tickInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should not panic; invalid expressions are logged and skipped.
	scheduler.Run(ctx)
}

func TestScheduler_CronRule(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	backend.SetLogger(log)

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:               "cron-rule",
		ScheduleExpression: "cron(0 12 * * ? *)",
		State:              "ENABLED",
	})
	require.NoError(t, err)

	// A 1-second tick interval - cron fires once per day, so no event in short test window.
	scheduler := eventbridge.NewScheduler(backend, log, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Just verify it doesn't panic.
	scheduler.Run(ctx)
}

func TestNewScheduler_DefaultTickInterval(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()

	// Passing 0 tick interval should use default.
	scheduler := eventbridge.NewScheduler(backend, log, 0)
	assert.NotNil(t, scheduler)
}
