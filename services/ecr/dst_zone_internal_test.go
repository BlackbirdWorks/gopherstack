package ecr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgeThreshold_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	now := time.Date(2026, time.November, 1, 12, 0, 0, 0, ny)

	tests := []struct {
		name  string
		unit  string
		count int
		want  time.Duration
	}{
		{name: "days", unit: "days", count: 1, want: 24 * time.Hour},
		{name: "weeks", unit: "weeks", count: 1, want: 7 * 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, now.Add(-tt.want).Equal(ageThreshold(now, tt.count, tt.unit)))
		})
	}
}
