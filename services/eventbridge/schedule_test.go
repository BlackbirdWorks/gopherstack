package eventbridge_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
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

// TestSchedule_CronDayOfWeek proves the AWS day-of-week convention (1-7,
// where 1 = Sunday) is honored, both numerically and via SUN-SAT names, and
// that it is NOT confused with Go's time.Weekday (0-6, where 0 = Sunday).
// A prior version of the matcher compared AWS field tokens directly against
// t.Weekday() with no offset, which silently fired schedules one weekday off
// and never matched named ranges like "MON-FRI" at all (parsing succeeded,
// but every candidate tick failed strconv.Atoi("MON") and was rejected).
func TestSchedule_CronDayOfWeek(t *testing.T) {
	t.Parallel()

	// utcMidnight returns a UTC midnight timestamp for 2024-01-<day> (or
	// 2024-02-<day> when feb is true), used verbatim as the fire time under test.
	utcMidnight := func(day int, feb bool) time.Time {
		month := time.January
		if feb {
			month = time.February
		}

		return time.Date(2024, month, day, 0, 0, 0, 0, time.UTC)
	}

	tests := []struct {
		day     time.Time
		name    string
		expr    string
		matches bool
	}{
		// 2024-01-14 is a Sunday, 2024-01-15 Monday, ... 2024-01-20 Saturday.
		{name: "AWS 1 is Sunday, not Monday", expr: "cron(0 0 ? * 1 *)", day: utcMidnight(14, false), matches: true},
		{name: "AWS 1 does not match Monday", expr: "cron(0 0 ? * 1 *)", day: utcMidnight(15, false), matches: false},
		{name: "AWS 2 is Monday", expr: "cron(0 0 ? * 2 *)", day: utcMidnight(15, false), matches: true},
		{name: "AWS 7 is Saturday", expr: "cron(0 0 ? * 7 *)", day: utcMidnight(20, false), matches: true},
		{name: "SUN name matches Sunday", expr: "cron(0 0 ? * SUN *)", day: utcMidnight(14, false), matches: true},
		{
			name:    "SUN name does not match Monday",
			expr:    "cron(0 0 ? * SUN *)",
			day:     utcMidnight(15, false),
			matches: false,
		},
		{
			name:    "MON-FRI matches Wednesday",
			expr:    "cron(0 0 ? * MON-FRI *)",
			day:     utcMidnight(17, false),
			matches: true,
		},
		{
			name:    "MON-FRI excludes Saturday",
			expr:    "cron(0 0 ? * MON-FRI *)",
			day:     utcMidnight(20, false),
			matches: false,
		},
		{name: "MON-FRI excludes Sunday", expr: "cron(0 0 ? * MON-FRI *)", day: utcMidnight(21, false), matches: false},
		{name: "JAN name matches January", expr: "cron(0 0 1 JAN ? *)", day: utcMidnight(1, false), matches: true},
		{name: "JAN name excludes February", expr: "cron(0 0 1 JAN ? *)", day: utcMidnight(1, true), matches: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sched, err := eventbridge.ParseScheduleExpressionForTest(tt.expr)
			require.NoError(t, err)

			// NextAfter scans forward from just before the candidate; if the
			// candidate matches, the next fire time returned is the candidate
			// itself (minute-resolution, so subtract a minute to search from).
			next := sched.NextAfterForTest(tt.day.Add(-time.Minute))
			if tt.matches {
				assert.True(t, next.Equal(tt.day), "expected fire at %v, got %v", tt.day, next)
			} else {
				assert.False(t, next.Equal(tt.day), "expected no fire at %v, but matched", tt.day)
			}
		})
	}
}
