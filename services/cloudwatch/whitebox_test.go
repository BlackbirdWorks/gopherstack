package cloudwatch

import (
	"testing"
	"time"
)

func TestParseISO8601Duration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{name: "minutes_only", in: "PT30M", want: 30 * time.Minute},
		{name: "hours_only", in: "PT4H", want: 4 * time.Hour},
		{name: "days_and_hours", in: "P2DT12H", want: 60 * time.Hour},
		{name: "days_only", in: "P7D", want: 7 * 24 * time.Hour},
		{name: "minimum_one_minute", in: "PT1M", want: time.Minute},
		{name: "maximum_fifteen_days", in: "P15D", want: 15 * 24 * time.Hour},
		{name: "seconds_component", in: "PT90S", want: 90 * time.Second},
		{name: "empty", in: "", wantErr: true},
		{name: "bare_p", in: "P", wantErr: true},
		{name: "bare_pt", in: "PT", wantErr: true},
		{name: "missing_p_prefix", in: "1H", wantErr: true},
		{name: "below_minimum", in: "PT30S", wantErr: true},
		{name: "above_maximum", in: "P16D", wantErr: true},
		{name: "calendar_month_rejected", in: "P1M", wantErr: true},
		{name: "calendar_year_rejected", in: "P1Y", wantErr: true},
		{name: "garbage", in: "not-a-duration", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseISO8601Duration(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseISO8601Duration(%q): want error, got nil", tt.in)
				}

				return
			}

			if err != nil {
				t.Fatalf("parseISO8601Duration(%q): unexpected error: %v", tt.in, err)
			}

			if got != tt.want {
				t.Fatalf("parseISO8601Duration(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestMuteCronMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		t    time.Time
		name string
		expr string
		want bool
	}{
		{
			name: "wildcard_matches_anything",
			expr: "cron(* * * * *)",
			t:    time.Date(2026, 3, 15, 7, 42, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "exact_minute_hour_match",
			expr: "cron(0 2 * * *)",
			t:    time.Date(2026, 3, 15, 2, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "exact_minute_hour_mismatch",
			expr: "cron(0 2 * * *)",
			t:    time.Date(2026, 3, 15, 2, 1, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "day_of_week_name",
			// 2026-03-15 is a Sunday.
			expr: "cron(0 2 * * SUN)",
			t:    time.Date(2026, 3, 15, 2, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "day_of_week_wrong_day",
			expr: "cron(0 2 * * MON)",
			t:    time.Date(2026, 3, 15, 2, 0, 0, 0, time.UTC),
			want: false,
		},
		{
			name: "day_of_month_first",
			expr: "cron(0 1 1 * *)",
			t:    time.Date(2026, 4, 1, 1, 0, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "step_every_ten_minutes",
			expr: "cron(0-30/10 2 * * *)",
			t:    time.Date(2026, 3, 15, 2, 20, 0, 0, time.UTC),
			want: true,
		},
		{
			name: "step_off_grid",
			expr: "cron(0-30/10 2 * * *)",
			t:    time.Date(2026, 3, 15, 2, 25, 0, 0, time.UTC),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, err := parseMuteCron(tt.expr)
			if err != nil {
				t.Fatalf("parseMuteCron(%q): %v", tt.expr, err)
			}

			if got := c.matches(tt.t); got != tt.want {
				t.Fatalf("(%q).matches(%v) = %v, want %v", tt.expr, tt.t, got, tt.want)
			}
		})
	}
}

func TestParseMuteCronFieldCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "valid_five_fields", expr: "cron(0 2 * * *)", wantErr: false},
		{name: "eventbridge_six_field_style_rejected", expr: "cron(0 2 * * ? *)", wantErr: true},
		{name: "too_few_fields", expr: "cron(0 2 * *)", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseMuteCron(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseMuteCron(%q) error = %v, wantErr %v", tt.expr, err, tt.wantErr)
			}
		})
	}
}

func TestMuteScheduleActive_At(t *testing.T) {
	t.Parallel()

	sched := AlarmMuteRuleSchedule{
		Expression: "at(2026-06-01T12:00)",
		Duration:   "PT1H",
	}

	tests := []struct {
		now  time.Time
		name string
		want bool
	}{
		{name: "before_window", now: time.Date(2026, 6, 1, 11, 59, 0, 0, time.UTC), want: false},
		{name: "at_start", now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC), want: true},
		{name: "mid_window", now: time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC), want: true},
		{name: "at_end_exclusive", now: time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC), want: false},
		{name: "after_window", now: time.Date(2026, 6, 1, 14, 0, 0, 0, time.UTC), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := muteScheduleActive(sched, tt.now)
			if err != nil {
				t.Fatalf("muteScheduleActive: %v", err)
			}

			if got != tt.want {
				t.Fatalf("muteScheduleActive(now=%v) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

func TestMuteScheduleActive_Cron(t *testing.T) {
	t.Parallel()

	// Recurring nightly maintenance window: 02:00 for 1 hour, every day.
	sched := AlarmMuteRuleSchedule{
		Expression: "cron(0 2 * * *)",
		Duration:   "PT1H",
	}

	tests := []struct {
		now  time.Time
		name string
		want bool
	}{
		{name: "just_after_occurrence", now: time.Date(2026, 3, 15, 2, 1, 0, 0, time.UTC), want: true},
		{name: "near_end_of_window", now: time.Date(2026, 3, 15, 2, 59, 0, 0, time.UTC), want: true},
		{name: "after_duration_elapsed", now: time.Date(2026, 3, 15, 3, 1, 0, 0, time.UTC), want: false},
		{name: "before_any_occurrence_today", now: time.Date(2026, 3, 15, 1, 0, 0, 0, time.UTC), want: false},
		{name: "next_day_occurrence_still_active", now: time.Date(2026, 3, 16, 2, 30, 0, 0, time.UTC), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := muteScheduleActive(sched, tt.now)
			if err != nil {
				t.Fatalf("muteScheduleActive: %v", err)
			}

			if got != tt.want {
				t.Fatalf("muteScheduleActive(now=%v) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

func TestMuteScheduleActive_Timezone(t *testing.T) {
	t.Parallel()

	// 09:00 America/New_York during EST (UTC-5) is 14:00 UTC.
	sched := AlarmMuteRuleSchedule{
		Expression: "cron(0 9 * * *)",
		Duration:   "PT1H",
		Timezone:   "America/New_York",
	}

	tests := []struct {
		now  time.Time
		name string
		want bool
	}{
		{name: "matches_in_utc_at_local_nine_am", now: time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC), want: true},
		{name: "does_not_match_nine_am_utc", now: time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := muteScheduleActive(sched, tt.now)
			if err != nil {
				t.Fatalf("muteScheduleActive: %v", err)
			}

			if got != tt.want {
				t.Fatalf("muteScheduleActive(now=%v) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

func TestMuteScheduleActive_InvalidTimezone(t *testing.T) {
	t.Parallel()

	sched := AlarmMuteRuleSchedule{
		Expression: "cron(0 2 * * *)",
		Duration:   "PT1H",
		Timezone:   "Not/A_Zone",
	}

	_, err := muteScheduleActive(sched, time.Now())
	if err == nil {
		t.Fatal("muteScheduleActive: want error for invalid timezone, got nil")
	}
}

func TestActiveMuteRule_ScopingAndWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 15, 2, 30, 0, 0, time.UTC)

	tests := []struct {
		name      string
		alarmName string
		rule      AlarmMuteRule
		wantMuted bool
	}{
		{
			name:      "active_window_and_targeted",
			alarmName: "cpu-alarm",
			rule: AlarmMuteRule{
				Name:       "maintenance",
				AlarmNames: []string{"cpu-alarm"},
				Schedule:   AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"},
			},
			wantMuted: true,
		},
		{
			name:      "targeted_but_outside_window",
			alarmName: "cpu-alarm",
			rule: AlarmMuteRule{
				Name:       "maintenance",
				AlarmNames: []string{"cpu-alarm"},
				Schedule:   AlarmMuteRuleSchedule{Expression: "cron(0 9 * * *)", Duration: "PT1H"},
			},
			wantMuted: false,
		},
		{
			name:      "active_window_but_not_targeted",
			alarmName: "other-alarm",
			rule: AlarmMuteRule{
				Name:       "maintenance",
				AlarmNames: []string{"cpu-alarm"},
				Schedule:   AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"},
			},
			wantMuted: false,
		},
		{
			name:      "active_window_but_rule_expired",
			alarmName: "cpu-alarm",
			rule: AlarmMuteRule{
				Name:       "maintenance",
				AlarmNames: []string{"cpu-alarm"},
				Schedule:   AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"},
				ExpireDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantMuted: false,
		},
		{
			name:      "active_window_but_not_yet_started",
			alarmName: "cpu-alarm",
			rule: AlarmMuteRule{
				Name:       "maintenance",
				AlarmNames: []string{"cpu-alarm"},
				Schedule:   AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"},
				StartDate:  time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			wantMuted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			b.PutAlarmMuteRuleInternal(&tt.rule)

			_, muted := b.activeMuteRule(tt.alarmName, now)
			if muted != tt.wantMuted {
				t.Fatalf("activeMuteRule(%q) = %v, want %v", tt.alarmName, muted, tt.wantMuted)
			}
		})
	}
}
