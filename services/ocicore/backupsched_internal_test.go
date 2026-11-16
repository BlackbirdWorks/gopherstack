package ocicore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextFire(t *testing.T) {
	t.Parallel()

	structured := func(period string, mod func(*BackupSchedule)) BackupSchedule {
		s := BackupSchedule{Period: period, OffsetType: "STRUCTURED"}
		if mod != nil {
			mod(&s)
		}

		return s
	}

	tests := []struct {
		name  string
		after string
		want  string
		s     BackupSchedule
	}{
		{
			"hour numeric", "2026-03-04T10:30:00Z", "2026-03-04T11:10:00Z",
			BackupSchedule{Period: periodHour, OffsetSeconds: 600},
		},
		{
			"hour same interval", "2026-03-04T10:05:00Z", "2026-03-04T10:10:00Z",
			BackupSchedule{Period: periodHour, OffsetSeconds: 600},
		},
		{
			"day structured", "2026-03-04T04:00:00Z", "2026-03-05T03:00:00Z",
			structured(periodDay, func(s *BackupSchedule) { s.HourOfDay = 3 }),
		},
		{
			"week structured", "2026-03-04T04:00:00Z", "2026-03-11T01:00:00Z",
			structured(periodWeek, func(s *BackupSchedule) { s.DayOfWeek, s.HourOfDay = "WEDNESDAY", 1 }),
		},
		{
			"week later day", "2026-03-04T04:00:00Z", "2026-03-07T00:00:00Z",
			structured(periodWeek, func(s *BackupSchedule) { s.DayOfWeek = "SATURDAY" }),
		},
		{
			"month clamp", "2026-02-01T00:00:00Z", "2026-02-28T00:00:00Z",
			structured(periodMonth, func(s *BackupSchedule) { s.DayOfMonth = 31 }),
		},
		{
			"year", "2026-07-01T00:00:00Z", "2027-06-15T02:00:00Z",
			structured(periodYear, func(s *BackupSchedule) { s.Month, s.DayOfMonth, s.HourOfDay = "JUNE", 15, 2 }),
		},
		{
			"regional", "2026-01-10T12:00:00Z", "2026-01-11T05:00:00Z",
			structured(periodDay, func(s *BackupSchedule) { s.TimeZone = tzRegional }),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			after, err := time.Parse(time.RFC3339, tc.after)
			require.NoError(t, err)

			want, err := time.Parse(time.RFC3339, tc.want)
			require.NoError(t, err)

			got := nextFire(tc.s, after)
			assert.True(t, got.Equal(want), "got %s want %s", got.UTC().Format(time.RFC3339), tc.want)
		})
	}
}
