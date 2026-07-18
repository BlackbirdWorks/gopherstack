package secretsmanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// NextCronTime — pure cron-expression engine
// ---------------------------------------------------------------------------

func TestCronNextTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		from    time.Time
		want    time.Time
		checkFn func(t *testing.T, next time.Time)
		name    string
		expr    string
		wantOk  bool
	}{
		{
			name:   "daily_midnight",
			expr:   "cron(0 0 * * ? *)",
			from:   time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "daily_noon",
			expr:   "cron(0 12 * * ? *)",
			from:   time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "daily_noon_after_noon",
			expr:   "cron(0 12 * * ? *)",
			from:   time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 16, 12, 0, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "weekly_sunday",
			expr:   "cron(0 0 ? * SUN *)",
			from:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), // Friday
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.Weekday(0), next.Weekday(), "must land on Sunday")
				assert.Equal(t, 0, next.Hour())
				assert.Equal(t, 0, next.Minute())
			},
		},
		{
			name:   "monthly_first_day",
			expr:   "cron(0 0 1 * ? *)",
			from:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 1, next.Day())
				assert.Equal(t, time.April, next.Month())
			},
		},
		{
			name:   "specific_month",
			expr:   "cron(0 0 1 JUN ? *)",
			from:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.June, next.Month())
				assert.Equal(t, 1, next.Day())
			},
		},
		{
			name:   "every_minute",
			expr:   "cron(* * * * ? *)",
			from:   time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC),
			want:   time.Date(2024, 3, 15, 10, 31, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "step_minutes",
			expr:   "cron(*/15 * * * ? *)",
			from:   time.Date(2024, 3, 15, 10, 7, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 15, next.Minute())
			},
		},
		{
			name:   "range_hours",
			expr:   "cron(0 8-17 * * ? *)",
			from:   time.Date(2024, 3, 15, 17, 30, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 16, 8, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 8, next.Hour())
			},
		},
		{
			name:   "comma_list",
			expr:   "cron(0 0 ? * MON,WED,FRI *)",
			from:   time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC), // Friday
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.Monday, next.Weekday())
			},
		},
		{
			name:   "year_boundary",
			expr:   "cron(0 0 1 1 ? *)",
			from:   time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 2025, next.Year())
				assert.Equal(t, time.January, next.Month())
				assert.Equal(t, 1, next.Day())
			},
		},
		{
			name:   "specific_year",
			expr:   "cron(0 0 1 1 ? 2025)",
			from:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 2025, next.Year())
			},
		},
		{
			name:   "exact_minute_boundary",
			expr:   "cron(0 0 * * ? *)",
			from:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.True(t, next.After(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)),
					"nextCronTime must return a time strictly after `from`")
			},
		},
		{
			name:   "leap_year_february",
			expr:   "cron(0 0 29 2 ? *)",
			from:   time.Date(2024, 2, 28, 0, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "not_firing_in_past",
			expr:   "cron(0 0 * * ? *)",
			from:   time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC),
			wantOk: true,
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.True(t, next.After(time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)))
			},
		},
		{
			name:   "expression_with_spaces",
			expr:   "  cron(0 0 * * ? *)  ",
			from:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
			wantOk: true,
		},
		{
			// 2024-03-15 is Friday (DOW=6 in AWS). cron(0 0 20 * FRI *) fires on day-20 OR any Friday.
			// From March 15 midnight, next Friday is March 22; next day-20 is March 20.
			name:   "both_dow_and_dom",
			expr:   "cron(0 0 20 * FRI *)",
			from:   time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			want:   time.Date(2024, 3, 20, 0, 0, 0, 0, time.UTC),
			wantOk: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next, ok := secretsmanager.NextCronTime(tt.expr, tt.from)
			require.Equal(t, tt.wantOk, ok)

			if !tt.want.IsZero() {
				assert.Equal(t, tt.want, next)
			}

			if tt.checkFn != nil {
				tt.checkFn(t, next)
			}
		})
	}
}

// TestCronNextTime_InvalidExpressionReturnsFalse verifies that invalid expressions
// return false.
func TestCronNextTime_InvalidExpressionReturnsFalse(t *testing.T) {
	t.Parallel()

	cases := []string{
		"rate(1 day)",
		"not-a-cron",
		"cron()",
		"cron(too few fields)",
		"cron(0 0 * * ?)",    // only 5 fields — missing year
		"cron(99 0 * * ? *)", // minute out of range
	}

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, expr := range cases {
		_, ok := secretsmanager.NextCronTime(expr, from)
		assert.False(t, ok, "expected false for %q", expr)
	}
}

// ---------------------------------------------------------------------------
// Cron field parsing — aliases/step/range
// ---------------------------------------------------------------------------

func TestCronParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		from    time.Time
		checkFn func(t *testing.T, next time.Time)
		name    string
		expr    string
	}{
		{
			// cron(0 0 1 MAR ? *) — first of March every year.
			name: "month_aliases",
			expr: "cron(0 0 1 MAR ? *)",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.March, next.Month())
				assert.Equal(t, 1, next.Day())
			},
		},
		{
			// cron(0 0 ? * MON *) — every Monday at midnight.
			// 2024-03-15 is Friday; next Monday is 2024-03-18.
			name: "dow_aliases",
			expr: "cron(0 0 ? * MON *)",
			from: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.Monday, next.Weekday())
			},
		},
		{
			// cron(0 */6 * * ? *) — every 6 hours (0, 6, 12, 18).
			name: "step_field",
			expr: "cron(0 */6 * * ? *)",
			from: time.Date(2024, 3, 15, 7, 0, 0, 0, time.UTC),
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, 12, next.Hour())
			},
		},
		{
			// cron(0 9 ? * 2-6 *) — Mon-Fri at 09:00.
			// 2024-03-15 is Friday. Friday DOW=6 (AWS). 09:00 hasn't passed (from = 08:00).
			name: "range_field",
			expr: "cron(0 9 ? * 2-6 *)",
			from: time.Date(2024, 3, 15, 8, 0, 0, 0, time.UTC),
			checkFn: func(t *testing.T, next time.Time) {
				t.Helper()
				assert.Equal(t, time.Friday, next.Weekday())
				assert.Equal(t, 9, next.Hour())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next, ok := secretsmanager.NextCronTime(tt.expr, tt.from)
			require.True(t, ok)
			tt.checkFn(t, next)
		})
	}
}
