package eventbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(t *testing.T, backend *eventbridge.InMemoryBackend)
		rule       eventbridge.PutRuleInput
		name       string
		ctxTimeout time.Duration
		runAsync   bool
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
					for _, entry := range backend.GetEventLog(context.Background()) {
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
				for _, e := range backend.GetEventLog(context.Background()) {
					assert.NotEqual(t, "aws.events", e.Source, "disabled rule should not fire events")
				}
			},
		},
		{
			name: "cron_rule_disabled_no_panic",
			rule: eventbridge.PutRuleInput{
				Name:               "cron-disabled-rule",
				ScheduleExpression: "cron(0 0 * * ? *)",
				State:              "DISABLED",
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

			backend := eventbridge.NewInMemoryBackend()

			_, err := backend.PutRule(context.Background(), tt.rule)
			require.NoError(t, err)

			scheduler := eventbridge.NewScheduler(backend, 50*time.Millisecond)
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

			backend := eventbridge.NewInMemoryBackend()
			scheduler := eventbridge.NewScheduler(backend, tt.tickInterval)
			assert.NotNil(t, scheduler)
		})
	}
}

func TestPutRule_ValidatesScheduleExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"valid rate", "rate(5 minutes)", false},
		{"valid cron", "cron(0 12 * * ? *)", false},
		{"invalid expression", "invalid(expression)", true},
		{"rate zero value", "rate(0 minutes)", true},
		{"rate negative", "rate(-1 minutes)", true},
		{"cron wrong fields", "cron(0 12 *)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := eventbridge.NewInMemoryBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:               "rule",
				ScheduleExpression: tt.expr,
				State:              "ENABLED",
			})
			if tt.wantErr {
				require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
