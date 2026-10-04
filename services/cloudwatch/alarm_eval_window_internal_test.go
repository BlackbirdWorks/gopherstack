package cloudwatch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAlarmEvaluationTime_WallClockAlignment(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.October, 7, 15, 47, 31, 0, time.UTC) // a Wednesday

	tests := []struct {
		window *AlarmEvaluationWindow
		want   time.Time
		name   string
		period int32
	}{
		{name: "sliding", period: 3600, want: now},
		{name: "nil-window", period: 3600, want: now, window: nil},
		{
			name: "hour-utc", period: 3600, window: &AlarmEvaluationWindow{WallClock: true},
			want: time.Date(2026, time.October, 7, 15, 0, 0, 0, time.UTC),
		},
		{
			name: "five-minutes", period: 300, window: &AlarmEvaluationWindow{WallClock: true},
			want: time.Date(2026, time.October, 7, 15, 45, 0, 0, time.UTC),
		},
		{
			name: "hour-offset-0530", period: 3600, window: &AlarmEvaluationWindow{WallClock: true, Timezone: "+05:30"},
			want: time.Date(2026, time.October, 7, 15, 30, 0, 0, time.UTC),
		},
		{
			name: "day-new-york", period: 86400,
			window: &AlarmEvaluationWindow{WallClock: true, Timezone: "America/New_York"},
			want:   time.Date(2026, time.October, 7, 4, 0, 0, 0, time.UTC),
		},
		{
			name: "week-monday", period: 604800, window: &AlarmEvaluationWindow{WallClock: true},
			want: time.Date(2026, time.October, 5, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := alarmEvaluationTime(MetricAlarm{Period: tt.period, EvaluationWindow: tt.window}, now)
			assert.True(t, tt.want.Equal(got), "want %s got %s", tt.want, got)
		})
	}
}
