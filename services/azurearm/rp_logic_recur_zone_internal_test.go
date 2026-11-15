package azurearm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func zoneTime(t *testing.T, name string, m time.Month, d, h, mi int) time.Time {
	t.Helper()

	loc, err := time.LoadLocation(name)
	require.NoError(t, err)

	return time.Date(2026, m, d, h, mi, 0, 0, loc).UTC()
}

func TestLogicRecurrenceTimeZone(t *testing.T) {
	t.Parallel()

	anchor := utc(2026, 1, 1, 0, 0)
	daily := func(zone, start string) map[string]any {
		return map[string]any{
			"frequency": "Day", "interval": int64(1), "timeZone": zone, "startTime": start,
			"schedule": map[string]any{"hours": []any{int64(9)}, "minutes": []any{int64(30)}},
		}
	}

	tests := []struct {
		after time.Time
		want  time.Time
		rec   map[string]any
		name  string
	}{
		{
			name: "windows name winter", after: utc(2026, 1, 10, 0, 0),
			rec:  daily("Pacific Standard Time", "2026-01-01T00:00:00"),
			want: zoneTime(t, "America/Los_Angeles", 1, 10, 9, 30),
		},
		{
			name: "wall time kept across spring forward", after: utc(2026, 3, 8, 17, 0),
			rec:  daily("Pacific Standard Time", "2026-01-01T00:00:00"),
			want: zoneTime(t, "America/Los_Angeles", 3, 9, 9, 30),
		},
		{
			name: "iana name", after: utc(2026, 7, 1, 0, 0),
			rec:  daily("Europe/Berlin", "2026-01-01T00:00:00"),
			want: zoneTime(t, "Europe/Berlin", 7, 1, 9, 30),
		},
		{
			name: "start time with offset is an instant", after: utc(2026, 1, 1, 0, 0),
			rec: map[string]any{
				"frequency": "Day", "interval": int64(1), "timeZone": "Tokyo Standard Time",
				"startTime": "2026-01-02T00:00:00Z",
			},
			want: utc(2026, 1, 2, 0, 0),
		},
		{
			name: "weekly schedule in zone", after: utc(2026, 1, 1, 0, 0),
			rec: map[string]any{
				"frequency": "Week", "interval": int64(1), "timeZone": "Tokyo Standard Time",
				"startTime": "2026-01-01T00:00:00",
				"schedule": map[string]any{
					"weekDays": []any{"Monday"}, "hours": []any{int64(8)}, "minutes": []any{int64(0)},
				},
			},
			want: zoneTime(t, "Asia/Tokyo", 1, 5, 8, 0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rc := parseLogicRecurrence(map[string]any{"recurrence": tc.rec}, anchor)
			got, ok := rc.next(tc.after)

			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValidateLogicRecurrenceTimeZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		zone    string
		wantErr bool
	}{
		{name: "utc", zone: "UTC"},
		{name: "windows", zone: "Pacific Standard Time"},
		{name: "iana", zone: "Asia/Tokyo"},
		{name: "unknown", zone: "Mars Standard Time", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateLogicRecurrence("t", map[string]any{"recurrence": map[string]any{
				"frequency": "Day", "interval": int64(1), "timeZone": tc.zone, "startTime": "2026-01-01T00:00:00",
			}})

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
