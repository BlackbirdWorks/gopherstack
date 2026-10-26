package ec2

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
		{"in range", 7, 7},
		{"negative", -3, 0},
		{"max", math.MaxInt32, math.MaxInt32},
		{"overflow", math.MaxInt32 + 1, math.MaxInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, clampInt32(tt.in))
		})
	}
}
