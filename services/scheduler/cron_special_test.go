package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func utcAt(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
}

func TestScheduler_Runner_CronSpecialTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		at   time.Time
		name string
		expr string
		want bool
	}{
		{name: "last_day_leap_feb", expr: "cron(0 12 L * ? *)", at: utcAt(2024, 2, 29, 12, 0), want: true},
		{name: "last_day_not_last", expr: "cron(0 12 L * ? *)", at: utcAt(2024, 2, 28, 12, 0)},
		{name: "last_minus_two", expr: "cron(30 23 L-2 * ? *)", at: utcAt(2026, 10, 29, 23, 30), want: true},
		{name: "last_minus_two_miss", expr: "cron(30 23 L-2 * ? *)", at: utcAt(2026, 10, 31, 23, 30)},
		{name: "last_weekday", expr: "cron(0 12 LW * ? *)", at: utcAt(2026, 10, 30, 12, 0), want: true},
		{name: "last_weekday_weekend", expr: "cron(0 12 LW * ? *)", at: utcAt(2026, 10, 31, 12, 0)},
		{name: "nearest_sat_to_fri", expr: "cron(0 12 15W * ? *)", at: utcAt(2026, 8, 14, 12, 0), want: true},
		{name: "nearest_sun_to_mon", expr: "cron(0 12 16W * ? *)", at: utcAt(2026, 8, 17, 12, 0), want: true},
		{name: "nearest_first_sat_to_mon", expr: "cron(0 12 1W * ? *)", at: utcAt(2026, 8, 3, 12, 0), want: true},
		{name: "nearest_last_sun_to_fri", expr: "cron(0 12 31W * ? *)", at: utcAt(2026, 5, 29, 12, 0), want: true},
		{name: "nearest_exact", expr: "cron(0 12 12W * ? *)", at: utcAt(2026, 8, 12, 12, 0), want: true},
		{name: "nearest_beyond_month", expr: "cron(0 12 31W * ? *)", at: utcAt(2026, 4, 30, 12, 0)},
		{name: "last_friday", expr: "cron(15 10 ? * 6L 2026)", at: utcAt(2026, 10, 30, 10, 15), want: true},
		{name: "not_last_friday", expr: "cron(15 10 ? * 6L 2026)", at: utcAt(2026, 10, 23, 10, 15)},
		{name: "second_tuesday", expr: "cron(0 12 ? * 3#2 *)", at: utcAt(2026, 10, 13, 12, 0), want: true},
		{name: "first_tuesday", expr: "cron(0 12 ? * 3#2 *)", at: utcAt(2026, 10, 6, 12, 0)},
		{name: "named_nth_weekday", expr: "cron(0 12 ? * FRI#1 *)", at: utcAt(2026, 10, 2, 12, 0), want: true},
		{name: "bare_l_is_saturday", expr: "cron(0 12 ? * L *)", at: utcAt(2026, 10, 3, 12, 0), want: true},
		{name: "list_with_last_day", expr: "cron(0 12 L,15 * ? *)", at: utcAt(2026, 10, 15, 12, 0), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				context.Background(), "special", "", tt.expr, "", "",
				scheduler.Target{
					ARN:     "arn:aws:lambda:us-east-1:000000000000:function:special-fn",
					RoleARN: "arn:aws:iam::000000000000:role/r",
				},
				"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			invoker := &mockLambdaInvoker{}
			runner := scheduler.NewRunner(backend)
			runner.SetLambdaInvoker(invoker)

			scheduler.CheckAndFireSchedules(t.Context(), runner, tt.at)
			assert.Equal(t, tt.want, len(invoker.Called()) == 1)
		})
	}
}
