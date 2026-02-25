package eventbridge_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedule_ParseRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expr     string
		wantErr  bool
		interval time.Duration
	}{
		{"1 second", "rate(1 second)", false, time.Second},
		{"5 seconds", "rate(5 seconds)", false, 5 * time.Second},
		{"1 minute", "rate(1 minute)", false, time.Minute},
		{"5 minutes", "rate(5 minutes)", false, 5 * time.Minute},
		{"1 hour", "rate(1 hour)", false, time.Hour},
		{"2 hours", "rate(2 hours)", false, 2 * time.Hour},
		{"1 day", "rate(1 day)", false, 24 * time.Hour},
		{"3 days", "rate(3 days)", false, 72 * time.Hour},
		{"invalid unit", "rate(5 weeks)", true, 0},
		{"zero value", "rate(0 minutes)", true, 0},
		{"negative value", "rate(-1 minutes)", true, 0},
		{"missing fields", "rate(minutes)", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sched, err := eventbridge.ParseScheduleExpressionForTest(tt.expr)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			// Verify next-fire is approximately one interval away from now.
			now := time.Now()
			next := sched.NextAfterForTest(now)
			elapsed := next.Sub(now)
			assert.Greater(t, elapsed, time.Duration(0), "next fire must be in the future")
			assert.LessOrEqual(t, elapsed, tt.interval+time.Second)
		})
	}
}

func TestSchedule_ParseCron(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"standard cron", "cron(0 12 * * ? *)", false},
		{"specific day", "cron(30 6 1 * ? *)", false},
		{"weekday", "cron(0 8 ? * MON-FRI *)", false},
		{"too few fields", "cron(0 12 * *)", true},
		{"too many fields", "cron(0 12 * * ? * extra)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := eventbridge.ParseScheduleExpressionForTest(tt.expr)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchedule_CronNextFire(t *testing.T) {
	t.Parallel()

	// cron(0 12 * * ? *) = every day at 12:00 UTC
	sched, err := eventbridge.ParseScheduleExpressionForTest("cron(0 12 * * ? *)")
	require.NoError(t, err)

	// If now is before noon, next should be today at noon.
	now := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	next := sched.NextAfterForTest(now)
	assert.Equal(t, time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC), next)

	// If now is after noon, next should be tomorrow at noon.
	nowAfter := time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC)
	nextAfter := sched.NextAfterForTest(nowAfter)
	assert.Equal(t, time.Date(2024, 1, 16, 12, 0, 0, 0, time.UTC), nextAfter)
}

func TestSchedule_UnsupportedExpression(t *testing.T) {
	t.Parallel()

	_, err := eventbridge.ParseScheduleExpressionForTest("at(2024-01-15T12:00:00)")
	require.Error(t, err)
}
