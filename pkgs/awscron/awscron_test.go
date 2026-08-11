package awscron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
)

func TestTokenValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		token  string
		kind   awscron.FieldKind
		want   int
		wantOK bool
	}{
		{name: "numeric minute", kind: awscron.FieldMinute, token: "30", want: 30, wantOK: true},
		{name: "month name", kind: awscron.FieldMonth, token: "mar", want: 3, wantOK: true},
		{
			name: "unknown month name falls through to numeric",
			kind: awscron.FieldMonth, token: "3", want: 3, wantOK: true,
		},
		{name: "invalid month name", kind: awscron.FieldMonth, token: "XYZ", wantOK: false},
		{name: "dow name SUN", kind: awscron.FieldDayOfWeek, token: "SUN", want: 0, wantOK: true},
		{name: "dow name SAT", kind: awscron.FieldDayOfWeek, token: "SAT", want: 6, wantOK: true},
		{name: "dow numeric AWS 1 is Sunday", kind: awscron.FieldDayOfWeek, token: "1", want: 0, wantOK: true},
		{name: "dow numeric AWS 7 is Saturday", kind: awscron.FieldDayOfWeek, token: "7", want: 6, wantOK: true},
		{name: "not a number", kind: awscron.FieldMinute, token: "abc", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := awscron.TokenValue(tt.kind, tt.token)
			require.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// matchFieldBounds returns the [min, max] this package defines for kind, so
// table cases below only need to name the field kind, not repeat its bounds.
func matchFieldBounds(kind awscron.FieldKind) (int, int) {
	switch kind {
	case awscron.FieldMinute:
		return awscron.MinuteMin, awscron.MinuteMax
	case awscron.FieldHour:
		return awscron.HourMin, awscron.HourMax
	case awscron.FieldDayOfMonth:
		return awscron.DayOfMonthMin, awscron.DayOfMonthMax
	case awscron.FieldMonth:
		return awscron.MonthMin, awscron.MonthMax
	case awscron.FieldDayOfWeek:
		return awscron.DayOfWeekMin, awscron.DayOfWeekMax
	case awscron.FieldYear:
		return awscron.YearMin, awscron.YearMax
	default:
		return 0, 0
	}
}

func TestMatchField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
		kind  awscron.FieldKind
		val   int
		want  bool
	}{
		{name: "wildcard star", kind: awscron.FieldMinute, field: "*", val: 42, want: true},
		{name: "wildcard question mark", kind: awscron.FieldDayOfMonth, field: "?", val: 1, want: true},
		{name: "exact match", kind: awscron.FieldHour, field: "5", val: 5, want: true},
		{name: "exact mismatch", kind: awscron.FieldHour, field: "5", val: 6, want: false},
		{name: "comma list match", kind: awscron.FieldMinute, field: "0,15,30,45", val: 30, want: true},
		{name: "comma list mismatch", kind: awscron.FieldMinute, field: "0,15,30,45", val: 20, want: false},
		{name: "range match", kind: awscron.FieldHour, field: "9-17", val: 12, want: true},
		{name: "range mismatch", kind: awscron.FieldHour, field: "9-17", val: 20, want: false},
		{name: "dow range wraps FRI-MON", kind: awscron.FieldDayOfWeek, field: "FRI-MON", val: 0, want: true},
		{name: "step match", kind: awscron.FieldMinute, field: "*/15", val: 45, want: true},
		{name: "step mismatch", kind: awscron.FieldMinute, field: "*/15", val: 20, want: false},
		{name: "range then step", kind: awscron.FieldMinute, field: "0-30/10", val: 20, want: true},
		{name: "range then step no match", kind: awscron.FieldMinute, field: "0-30/10", val: 25, want: false},
		{name: "zero step never matches", kind: awscron.FieldMinute, field: "5/0", val: 5, want: false},
		{name: "malformed range", kind: awscron.FieldHour, field: "9-17-20", val: 9, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fieldMin, fieldMax := matchFieldBounds(tt.kind)
			got := awscron.MatchField(tt.kind, tt.field, tt.val, fieldMin, fieldMax)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchDayFields(t *testing.T) {
	t.Parallel()

	// A Wednesday, so day-of-month 15 and day-of-week WED both hold.
	wed15 := time.Date(2024, time.May, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		t          time.Time
		name       string
		dayOfMonth string
		dayOfWeek  string
		want       bool
	}{
		{name: "both wildcard", dayOfMonth: "*", dayOfWeek: "?", t: wed15, want: true},
		{name: "dom wildcard defers to dow", dayOfMonth: "?", dayOfWeek: "MON", t: wed15, want: false},
		{name: "dow wildcard defers to dom", dayOfMonth: "15", dayOfWeek: "?", t: wed15, want: true},
		{name: "both concrete either matches", dayOfMonth: "1", dayOfWeek: "WED", t: wed15, want: true},
		{name: "both concrete neither matches", dayOfMonth: "1", dayOfWeek: "MON", t: wed15, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := awscron.MatchDayFields(tt.dayOfMonth, tt.dayOfWeek, tt.t)
			assert.Equal(t, tt.want, got)
		})
	}
}
