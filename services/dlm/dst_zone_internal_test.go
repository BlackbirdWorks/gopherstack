package dlm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetainCutoff_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	now := time.Date(2026, time.November, 1, 12, 0, 0, 0, ny)

	tests := []struct {
		name string
		unit string
		want time.Duration
	}{
		{name: "days", unit: "DAYS", want: 24 * time.Hour},
		{name: "weeks", unit: "WEEKS", want: 7 * 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := retainCutoff(map[string]any{"Interval": float64(1), "IntervalUnit": tt.unit}, now)
			require.True(t, ok)
			assert.True(t, now.Add(-tt.want).Equal(got))
		})
	}
}
