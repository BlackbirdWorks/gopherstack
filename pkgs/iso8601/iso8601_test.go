package iso8601_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{name: "minutes", in: "PT1M", want: time.Minute},
		{name: "seconds", in: "PT30S", want: 30 * time.Second},
		{name: "days", in: "P14D", want: 14 * 24 * time.Hour},
		{name: "hours minutes seconds", in: "PT5M30S", want: 5*time.Minute + 30*time.Second},
		{
			name: "days and time", in: "P1DT2H3M4S",
			want: 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second,
		},
		{
			name: "fractional seconds", in: "PT1.5S",
			want: time.Second + 500*time.Millisecond,
		},
		{name: "empty string is invalid", in: "", wantErr: true},
		{name: "missing P prefix is invalid", in: "1D", wantErr: true},
		{name: "garbage is invalid", in: "not-a-duration", wantErr: true},
		{
			name: "huge sentinel value clamps to max time.Duration",
			in:   "P10675199DT2H48M5.4775807S",
			want: math.MaxInt64,
		},
		{
			name: "exactly 2^63 nanoseconds clamps rather than going negative",
			in:   "PT9223372036.854775808S",
			want: math.MaxInt64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := iso8601.Parse(tt.in)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, iso8601.ErrInvalidDuration)

				return
			}

			require.NoError(t, err)
			assert.GreaterOrEqual(t, got, time.Duration(0), "parsed duration must never be negative")
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		in   time.Duration
	}{
		{name: "zero", in: 0, want: iso8601.Zero},
		{name: "negative clamps to zero form", in: -time.Second, want: iso8601.Zero},
		{name: "minute", in: time.Minute, want: "PT1M"},
		{name: "seconds", in: 30 * time.Second, want: "PT30S"},
		{name: "days", in: 14 * 24 * time.Hour, want: "P14D"},
		{
			name: "days and time",
			in:   24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second,
			want: "P1DT2H3M4S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, iso8601.Format(tt.in))
		})
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	durations := []time.Duration{
		time.Second, 30 * time.Second, time.Minute, 5 * time.Minute,
		time.Hour, 24 * time.Hour, 14 * 24 * time.Hour,
		time.Hour + 2*time.Minute + 3*time.Second,
	}

	for _, d := range durations {
		formatted := iso8601.Format(d)

		parsed, err := iso8601.Parse(formatted)
		require.NoError(t, err, "formatted value %q for %s should parse back", formatted, d)
		assert.Equal(t, d, parsed, "round-trip mismatch for %s (formatted as %q)", d, formatted)
	}
}
