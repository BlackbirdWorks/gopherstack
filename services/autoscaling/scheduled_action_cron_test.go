package autoscaling

import (
	"testing"
	"time"
)

// TestParseRecurrence locks the 5-field validation of parseRecurrence: exactly
// "minute hour day-of-month month day-of-week" is accepted, anything else
// (too few/many fields) is rejected.
func TestParseRecurrence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "valid_daily", expr: "0 10 * * *", wantErr: false},
		{name: "valid_all_wildcards", expr: "* * * * *", wantErr: false},
		{name: "too_few_fields", expr: "0 10 * *", wantErr: true},
		{name: "too_many_fields_eventbridge_style", expr: "0 10 * * ? *", wantErr: true},
		{name: "empty", expr: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseRecurrence(tt.expr)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseRecurrence(%q): want error, got nil", tt.expr)
				}

				return
			}

			if err != nil {
				t.Fatalf("parseRecurrence(%q): unexpected error: %v", tt.expr, err)
			}
		})
	}
}

// TestRecurrenceScheduleMatches locks the field-matching semantics (wildcard,
// exact, list, range, step) that AWS documents for the Recurrence cron format.
func TestRecurrenceScheduleMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		t    time.Time
		name string
		expr string
		want bool
	}{
		{
			name: "wildcard_matches_anything",
			expr: "* * * * *",
			t:    time.Date(2026, 3, 15, 7, 42, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "exact_minute_hour_match",
			expr: "30 10 * * *",
			t:    time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "exact_minute_hour_mismatch",
			expr: "30 10 * * *",
			t:    time.Date(2026, 3, 15, 10, 31, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "day_of_week_list",
			// Sunday(0) or Saturday(6) -- 2026-03-15 is a Sunday.
			expr: "0 0 * * 0,6",
			t:    time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "day_of_week_list_mismatch",
			// 2026-03-16 is a Monday.
			expr: "0 0 * * 0,6",
			t:    time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "hour_range",
			expr: "0 9-17 * * *",
			t:    time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "hour_range_mismatch",
			expr: "0 9-17 * * *",
			t:    time.Date(2026, 3, 15, 20, 0, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "minute_step",
			expr: "*/15 * * * *",
			t:    time.Date(2026, 3, 15, 12, 30, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "minute_step_mismatch",
			expr: "*/15 * * * *",
			t:    time.Date(2026, 3, 15, 12, 31, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "month_exact",
			expr: "0 0 1 6 *",
			t:    time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schedule, err := parseRecurrence(tt.expr)
			if err != nil {
				t.Fatalf("parseRecurrence(%q): unexpected error: %v", tt.expr, err)
			}

			if got := schedule.matches(tt.t); got != tt.want {
				t.Errorf("matches(%v) for %q = %v, want %v", tt.t, tt.expr, got, tt.want)
			}
		})
	}
}

// TestRecurrenceScheduleNextAfter locks that NextAfter returns the correct
// next occurrence strictly after the baseline, at minute resolution.
func TestRecurrenceScheduleNextAfter(t *testing.T) {
	t.Parallel()

	schedule, err := parseRecurrence("0 10 * * *")
	if err != nil {
		t.Fatalf("parseRecurrence: unexpected error: %v", err)
	}

	baseline := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC)

	next := schedule.NextAfter(baseline)

	want := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("NextAfter(%v) = %v, want %v", baseline, next, want)
	}

	// Baseline already past today's occurrence rolls to tomorrow.
	baseline2 := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)

	next2 := schedule.NextAfter(baseline2)

	want2 := time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC)
	if !next2.Equal(want2) {
		t.Errorf("NextAfter(%v) = %v, want %v", baseline2, next2, want2)
	}
}
