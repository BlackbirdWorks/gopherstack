package ssm

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClampInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int32
	}{
		{name: "zero", in: 0, want: 0},
		{name: "negative", in: -5, want: -5},
		{name: "overflow", in: math.MaxInt32 + 10, want: math.MaxInt32},
		{name: "underflow", in: math.MinInt32 - 10, want: math.MinInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clampInt32(tt.in))
		})
	}
}
