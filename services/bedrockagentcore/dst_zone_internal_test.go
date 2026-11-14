package bedrockagentcore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventExpired_DSTZone(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		ts   time.Time
		name string
		age  time.Duration
		want bool
	}{
		{
			name: "fall back 24h30m old is expired",
			ts:   time.Date(2026, time.November, 1, 0, 0, 0, 0, ny),
			age:  24*time.Hour + 30*time.Minute, want: true,
		},
		{
			name: "spring forward 23h30m old is live",
			ts:   time.Date(2026, time.March, 8, 0, 0, 0, 0, ny),
			age:  23*time.Hour + 30*time.Minute, want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &Event{Timestamp: tt.ts}
			assert.Equal(t, tt.want, e.expired(MemoryInfo{ExpiryDays: 1}, tt.ts.Add(tt.age)))
		})
	}
}
