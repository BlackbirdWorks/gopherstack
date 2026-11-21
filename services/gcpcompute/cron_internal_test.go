package gcpcompute

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCronMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "every minute", expr: "* * * * *"},
		{name: "list range step", expr: "0,15,30-40/5 */2 1-15 JAN,MAR MON-FRI"},
		{name: "sunday seven", expr: "0 0 * * 7"},
		{name: "four fields", expr: "* * * *", wantErr: true},
		{name: "six fields", expr: "* * * * * *", wantErr: true},
		{name: "empty", expr: "", wantErr: true},
		{name: "out of range", expr: "60 * * * *", wantErr: true},
		{name: "reversed", expr: "30-10 * * * *", wantErr: true},
		{name: "zero step", expr: "*/0 * * * *", wantErr: true},
		{name: "negative", expr: "-1 * * * *", wantErr: true},
		{name: "letters", expr: "a * * * *", wantErr: true},
		{name: "empty list item", expr: "1,,2 * * * *", wantErr: true},
		{name: "huge step on value", expr: "5/9223372036854775807 * * * *"},
		{name: "overflowing step", expr: "5/99999999999999999999 * * * *", wantErr: true},
		{name: "oversized", expr: strings.Repeat("1,", 200) + "1 * * * *", wantErr: true},
		{name: "nul", expr: "\x00 * * * *", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseCron(tt.expr)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestCronFiresOnceAcrossFallBack(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	c, err := parseCron("30 1 * * *")
	require.NoError(t, err)

	// 2026-11-01: 01:30 EDT (05:30Z) and again 01:30 EST (06:30Z).
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	second := first.Add(time.Hour)

	at, ok := c.latestFire(loc, first.Add(-time.Hour), second.Add(10*time.Minute))
	require.True(t, ok)
	assert.Equal(t, first, at, "the repeated 01:30 is not a second fire")

	_, ok = c.latestFire(loc, first, second.Add(10*time.Minute))
	assert.False(t, ok, "nothing fires between the two passes over 01:30")
}

func TestCronSpringForwardAndZones(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	c, err := parseCron("30 2 * * *")
	require.NoError(t, err)

	// 2026-03-08: 02:30 does not exist in New York.
	from := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	_, ok := c.latestFire(ny, from, from.Add(24*time.Hour))
	assert.False(t, ok, "a time the clock skips does not fire")

	next, ok := c.nextFire(ny, from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC), next)

	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	at, ok := c.latestFire(tokyo, from, from.Add(24*time.Hour))
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 3, 8, 17, 30, 0, 0, time.UTC), at)
}

func TestCronHugeStepKeepsOnlyTheValue(t *testing.T) {
	t.Parallel()

	c, err := parseCron("5/9223372036854775807 * * * *")
	require.NoError(t, err)
	assert.Equal(t, uint64(1)<<5, c.min)
}
