package asl

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapCopyCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want int
	}{
		{
			name: "zero_size",
			size: 0,
			want: 1,
		},
		{
			name: "small_size",
			size: 7,
			want: 8,
		},
		{
			name: "max_int_size",
			size: math.MaxInt,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mapCopyCapacity(tt.size)
			assert.Equal(t, tt.want, got)
		})
	}
}
