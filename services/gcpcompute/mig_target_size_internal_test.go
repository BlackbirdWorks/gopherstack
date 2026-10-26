package gcpcompute

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMIGTargetSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int64
		want int
	}{
		{"in range", 5, 5},
		{"negative", -1, 0},
		{"overflow", math.MaxInt64, math.MaxInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, migTargetSize(tt.in))
		})
	}
}
