package quotalimit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

func TestGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn      quotalimit.Func
		name    string
		current int
		want    bool
	}{
		{name: "unset", fn: nil, current: 99, want: false},
		{
			name: "unknown quota", current: 99, want: false,
			fn: func(_, _, _, _ string) (float64, bool) { return 0, false },
		},
		{name: "below", fn: func(_, _, _, _ string) (float64, bool) { return 3, true }, current: 2, want: false},
		{name: "at limit", fn: func(_, _, _, _ string) (float64, bool) { return 3, true }, current: 3, want: true},
		{name: "zero limit", fn: func(_, _, _, _ string) (float64, bool) { return 0, true }, current: 0, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var g quotalimit.Gate

			g.Set(tc.fn)

			assert.Equal(t, tc.want, g.Lookup("a", "r", "s", "q").Reached(tc.current))
		})
	}
}

func TestNilGate(t *testing.T) {
	t.Parallel()

	var g *quotalimit.Gate

	assert.False(t, g.Lookup("a", "r", "s", "q").Reached(1))
}
