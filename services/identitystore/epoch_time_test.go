package identitystore //nolint:testpackage // epochTime is unexported; white-box unit tests of its wire encoding.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEpochTimeMarshalJSON verifies epochTime renders as an AWS
// JSON-protocol epoch-seconds number, matching how the real identitystore
// SDK deserializes CreatedAt/UpdatedAt (see
// aws-sdk-go-v2/service/identitystore's deserializers.go, which parses
// these fields with smithytime.ParseEpochSeconds on a JSON float64).
func TestEpochTimeMarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "zero_time_marshals_to_zero",
			in:   time.Time{},
			want: "0",
		},
		{
			name: "whole_seconds",
			in:   time.Unix(1_700_000_000, 0).UTC(),
			want: "1700000000",
		},
		{
			name: "sub_second_precision_preserved",
			in:   time.Unix(1_700_000_000, 500_000_000).UTC(),
			want: "1700000000.5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(epochTime(tt.in))
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestEpochTimeUnmarshalJSON verifies epochTime parses an AWS
// JSON-protocol epoch-seconds number back into the equivalent time.Time,
// and rejects non-numeric input (e.g. an RFC3339 string, which is what
// Go's default time.Time JSON encoding would have produced -- exactly the
// bug class this type exists to avoid).
func TestEpochTimeUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    time.Time
		name    string
		in      string
		wantErr bool
	}{
		{
			name: "whole_seconds",
			in:   "1700000000",
			want: time.Unix(1_700_000_000, 0).UTC(),
		},
		{
			name: "fractional_seconds",
			in:   "1700000000.5",
			want: time.Unix(1_700_000_000, 500_000_000).UTC(),
		},
		{
			name:    "rfc3339_string_rejected",
			in:      `"2024-01-01T00:00:00Z"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got epochTime

			err := json.Unmarshal([]byte(tt.in), &got)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.True(t, tt.want.Equal(time.Time(got)), "got %v, want %v", time.Time(got), tt.want)
		})
	}
}

// TestEpochTimeRoundTrip verifies marshal->unmarshal preserves the instant
// within sub-millisecond precision (the wire format is a float64 seconds
// count, which is lossy at full nanosecond scale for present-day epoch
// values -- see the note in persistence_test.go's timestamp round-trip
// subtest).
func TestEpochTimeRoundTrip(t *testing.T) {
	t.Parallel()

	original := time.Now().UTC()

	data, err := json.Marshal(epochTime(original))
	require.NoError(t, err)

	var restored epochTime

	require.NoError(t, json.Unmarshal(data, &restored))
	assert.WithinDuration(t, original, time.Time(restored), time.Millisecond)
}
