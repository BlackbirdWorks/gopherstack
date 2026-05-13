package asl

import (
	"math"
	"testing"
	"time"

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

func TestComputeRetryDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		intervalSeconds int
		backoffRate     float64
		attempts        int
		want            time.Duration
	}{
		{
			name:            "normal_backoff",
			intervalSeconds: 2,
			backoffRate:     2,
			attempts:        3,
			want:            16 * time.Second,
		},
		{
			name:            "negative_interval_clamped_to_zero",
			intervalSeconds: -1,
			backoffRate:     2,
			attempts:        1,
			want:            0,
		},
		{
			name:            "overflow_backoff_clamped_to_max",
			intervalSeconds: 2,
			backoffRate:     math.MaxFloat64,
			attempts:        2,
			want:            maxRetryDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := computeRetryDelay(tt.intervalSeconds, tt.backoffRate, tt.attempts)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJSONPathCacheBounded(t *testing.T) {
	t.Parallel()

	cache := newJSONPathCache(2)

	getCachedJSONPathParts("a.b", cache)
	getCachedJSONPathParts("c.d", cache)
	getCachedJSONPathParts("e.f", cache)

	assert.EqualValues(t, 2, cache.size.Load())

	_, found := cache.load("e.f")
	assert.False(t, found)
}
