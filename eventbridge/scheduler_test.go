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

func TestScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rule       eventbridge.PutRuleInput
		ctxTimeout time.Duration
		runAsync   bool
		check      func(t *testing.T, backend *eventbridge.InMemoryBackend)
	}{
		{
			name: "fires_rate_rule",
			rule: eventbridge.PutRuleInput{
				Name:               "sched-rule",
				ScheduleExpression: "rate(1 second)",
				State:              "ENABLED",
			},
			ctxTimeout: 5 * time.Second,
			runAsync:   true,
			check: func(t *testing.T, backend *eventbridge.InMemoryBackend) {
				t.Helper()
				require.Eventually(t, func() bool {
					for _, entry := range backend.GetEventLog() {
						if entry.Source == "aws.events" {
							return true
						}
					}
					return false
				}, 5*time.Second, 100*time.Millisecond, "expected at least one scheduled event to be fired")
			},
		},
		{
			name: "skips_disabled_rule",
			rule: eventbridge.PutRuleInput{
				Name:               "disabled-sched-rule",
				ScheduleExpression: "rate(1 minute)",
				State:              "DISABLED",
			},
			ctxTimeout: 200 * time.Millisecond,
			runAsync:   true,
			check: func(t *testing.T, backend *eventbridge.InMemoryBackend) {
				t.Helper()
				// Wait for the context to expire and then a little more to confirm no events fired.
				time.Sleep(300 * time.Millisecond)
				for _, e := range backend.GetEventLog() {
					assert.NotEqual(t, "aws.events", e.Source, "disabled rule should not fire events")
				}
			},
		},
		{
			name: "invalid_expression_no_panic",
			rule: eventbridge.PutRuleInput{
				Name:               "bad-sched-rule",
				ScheduleExpression: "invalid(expression)",
				State:              "ENABLED",
			},
			ctxTimeout: 200 * time.Millisecond,
			runAsync:   false,
		},
		{
			name: "cron_rule_no_panic",
			rule: eventbridge.PutRuleInput{
				Name:               "cron-rule",
				ScheduleExpression: "cron(0 12 * * ? *)",
				State:              "ENABLED",
			},
			ctxTimeout: 100 * time.Millisecond,
			runAsync:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := logger.NewLogger(slog.LevelDebug)
			backend := eventbridge.NewInMemoryBackend()
			backend.SetLogger(log)

			_, err := backend.PutRule(tt.rule)
			require.NoError(t, err)

			scheduler := eventbridge.NewScheduler(backend, log, 50*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), tt.ctxTimeout)
			defer cancel()

			if tt.runAsync {
				go scheduler.Run(ctx)
			} else {
				scheduler.Run(ctx)
			}

			if tt.check != nil {
				tt.check(t, backend)
			}
		})
	}
}

func TestNewScheduler_DefaultTickInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tickInterval time.Duration
	}{
		{
			name:         "zero_interval_uses_default",
			tickInterval: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := logger.NewLogger(slog.LevelDebug)
			backend := eventbridge.NewInMemoryBackend()
			scheduler := eventbridge.NewScheduler(backend, log, tt.tickInterval)
			assert.NotNil(t, scheduler)
		})
	}
}
