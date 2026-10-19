package parquetio

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClampInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int64
		want int32
	}{
		{"in range", 42, 42},
		{"above", math.MaxInt32 + 1, math.MaxInt32},
		{"below", math.MinInt32 - 1, math.MinInt32},
		{"max int64", math.MaxInt64, math.MaxInt32},
		{"min int64", math.MinInt64, math.MinInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clampInt32(tt.in))
		})
	}
}
