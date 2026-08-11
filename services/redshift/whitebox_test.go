package redshift

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedAfter is 2026-01-15T10:30:00Z, a Thursday. Every nextInvocations test
// scans forward from a fixed point so results are deterministic.
var fixedAfter = time.Date(2026, time.January, 15, 10, 30, 0, 0, time.UTC) //nolint:gochecknoglobals // test fixture

func TestNextInvocations_At(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
		want     []time.Time
	}{
		{
			name:     "future",
			schedule: "at(2099-01-01T00:00:00)",
			want:     []time.Time{time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:     "past",
			schedule: "at(2000-01-01T00:00:00)",
			want:     nil,
		},
		{
			name:     "equal to after is not past",
			schedule: "at(2026-01-15T10:30:00)",
			want:     []time.Time{fixedAfter},
		},
		{
			name:     "malformed timestamp",
			schedule: "at(not-a-date)",
			want:     nil,
		},
		{
			name:     "empty inner",
			schedule: "at()",
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNextInvocations_Unsupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
	}{
		{name: "rate expression", schedule: "rate(1 day)"},
		{name: "rate with plural unit", schedule: "rate(5 minutes)"},
		{name: "empty string", schedule: ""},
		{name: "garbage", schedule: "not a schedule at all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			assert.Nil(t, got, "real Redshift does not accept rate() for ScheduledAction.Schedule")
		})
	}
}

func TestNextInvocations_CronMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
	}{
		{name: "too few fields", schedule: "cron(0 12 * *)"},
		{name: "too many fields", schedule: "cron(0 12 * * ? * extra)"},
		{name: "empty inner", schedule: "cron()"},
		{name: "out of range values never match", schedule: "cron(99 25 99 13 ? *)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			assert.Empty(t, got)
		})
	}
}

func TestNextInvocations_CronSpecialCharsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
	}{
		{name: "L last day of month", schedule: "cron(0 0 L * ? *)"},
		{name: "W nearest weekday", schedule: "cron(0 0 1W * ? *)"},
		{name: "hash nth weekday", schedule: "cron(0 0 ? * 1#3 *)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			assert.Empty(t, got, "unsupported syntax must never fabricate a match")
		})
	}
}

func TestNextInvocations_CronWildcardsAndLists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkMinute func(t *testing.T, minute int)
		name        string
		schedule    string
	}{
		{
			name:     "every minute",
			schedule: "cron(* * * * ? *)",
			checkMinute: func(t *testing.T, minute int) {
				t.Helper()
				assert.GreaterOrEqual(t, minute, 0)
				assert.LessOrEqual(t, minute, 59)
			},
		},
		{
			name:     "comma list",
			schedule: "cron(0,30 * * * ? *)",
			checkMinute: func(t *testing.T, minute int) {
				t.Helper()
				assert.Contains(t, []int{0, 30}, minute)
			},
		},
		{
			name:     "step wildcard",
			schedule: "cron(*/15 * * * ? *)",
			checkMinute: func(t *testing.T, minute int) {
				t.Helper()
				assert.Equal(t, 0, minute%15)
			},
		},
		{
			name:     "range with step",
			schedule: "cron(0-30/10 * * * ? *)",
			checkMinute: func(t *testing.T, minute int) {
				t.Helper()
				assert.Contains(t, []int{0, 10, 20, 30}, minute)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			require.NotEmpty(t, got, "expected at least one match within the scan window")

			for _, ts := range got {
				tt.checkMinute(t, ts.Minute())
			}
		})
	}
}

func TestNextInvocations_CronRangeField(t *testing.T) {
	t.Parallel()

	got := nextInvocations("cron(0 9-17 * * ? *)", fixedAfter)
	require.NotEmpty(t, got)

	for _, ts := range got {
		assert.GreaterOrEqual(t, ts.Hour(), 9)
		assert.LessOrEqual(t, ts.Hour(), 17)
		assert.Equal(t, 0, ts.Minute())
	}
}

func TestNextInvocations_DayOfWeekNumbering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		schedule    string
		wantWeekday time.Weekday
	}{
		{name: "AWS 1 is Sunday, not Monday", schedule: "cron(0 0 ? * 1 *)", wantWeekday: time.Sunday},
		{name: "AWS 2 is Monday", schedule: "cron(0 0 ? * 2 *)", wantWeekday: time.Monday},
		{name: "AWS 7 is Saturday", schedule: "cron(0 0 ? * 7 *)", wantWeekday: time.Saturday},
		{name: "SUN name matches Sunday", schedule: "cron(0 0 ? * SUN *)", wantWeekday: time.Sunday},
		{name: "MON-FRI range", schedule: "cron(0 0 ? * MON-FRI *)", wantWeekday: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextInvocations(tt.schedule, fixedAfter)
			require.NotEmpty(t, got)

			for _, ts := range got {
				assert.Equal(t, 0, ts.Hour())
				assert.Equal(t, 0, ts.Minute())

				if tt.wantWeekday >= 0 {
					assert.Equal(t, tt.wantWeekday, ts.Weekday())
				} else {
					assert.NotEqual(t, time.Saturday, ts.Weekday())
					assert.NotEqual(t, time.Sunday, ts.Weekday())
				}
			}
		})
	}
}

func TestNextInvocations_DayOfMonthVsDayOfWeekUnion(t *testing.T) {
	t.Parallel()

	// Both fields concrete: AWS fires when EITHER matches, not only when both do.
	got := nextInvocations("cron(0 0 1 * MON *)", fixedAfter)
	require.NotEmpty(t, got)

	for _, ts := range got {
		assert.True(t, ts.Day() == 1 || ts.Weekday() == time.Monday)
	}
}

func TestNextInvocations_MonthRollover(t *testing.T) {
	t.Parallel()

	got := nextInvocations("cron(0 0 31 * ? *)", fixedAfter)
	require.NotEmpty(t, got)

	for _, ts := range got {
		assert.Equal(t, 31, ts.Day())
		assert.NotEqual(t, time.February, ts.Month())
		assert.NotEqual(t, time.April, ts.Month())
		assert.NotEqual(t, time.June, ts.Month())
	}
}

func TestNextInvocations_LeapYearFeb29(t *testing.T) {
	t.Parallel()

	// 2028 is the next leap year after fixedAfter's 2026; start close enough
	// to it that Feb 29 2028 falls inside the 2-year forward scan window.
	after := time.Date(2027, time.June, 1, 0, 0, 0, 0, time.UTC)

	got := nextInvocations("cron(0 0 29 2 ? *)", after)
	require.NotEmpty(t, got, "leap day must be reachable within the scan window")
	assert.Equal(t, time.Date(2028, time.February, 29, 0, 0, 0, 0, time.UTC), got[0])
}

func TestNextInvocations_MaxCap(t *testing.T) {
	t.Parallel()

	got := nextInvocations("cron(* * * * ? *)", fixedAfter)
	require.Len(t, got, maxNextInvocations)

	want := fixedAfter.Add(time.Minute)
	for _, ts := range got {
		assert.Equal(t, want, ts)
		want = want.Add(time.Minute)
	}
}
