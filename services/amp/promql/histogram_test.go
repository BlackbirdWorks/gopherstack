package promql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloatHistogramQuantile(t *testing.T) {
	t.Parallel()

	zeroPos := &FloatHistogram{
		ZeroThreshold: 0.5, ZeroCount: 2, Count: 4, Sum: 3,
		PositiveSpans: []Span{{Offset: 1, Length: 1}}, PositiveBuckets: []float64{2},
	}
	neg := &FloatHistogram{
		Count: 4, Sum: -6, NegativeSpans: []Span{{Offset: 1, Length: 1}}, NegativeBuckets: []float64{4},
	}

	tests := []struct {
		name string
		h    *FloatHistogram
		q    float64
		want float64
	}{
		{name: "zero bucket linear", h: zeroPos, q: 0.25, want: 0.25},
		{name: "positive geometric", h: zeroPos, q: 0.75, want: math.Sqrt2},
		{name: "top of last bucket", h: zeroPos, q: 1, want: 2},
		{name: "negative bucket", h: neg, q: 0.5, want: -math.Sqrt2},
		{name: "below zero", h: zeroPos, q: -1, want: math.Inf(-1)},
		{name: "above one", h: zeroPos, q: 2, want: math.Inf(1)},
		{name: "empty", h: &FloatHistogram{}, q: 0.5, want: math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.h.Quantile(tt.q)
			if math.IsNaN(tt.want) {
				assert.True(t, math.IsNaN(got))

				return
			}

			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

func TestFloatHistogramValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		h    FloatHistogram
		want bool
	}{
		{name: "ok", h: FloatHistogram{PositiveSpans: []Span{{Length: 2}}, PositiveBuckets: []float64{1, 1}}, want: true},
		{name: "mismatch", h: FloatHistogram{PositiveSpans: []Span{{Length: 2}}, PositiveBuckets: []float64{1}}},
		{name: "schema high", h: FloatHistogram{Schema: MaxSchema + 1}},
		{name: "schema low", h: FloatHistogram{Schema: MinSchema - 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.h.Valid())
		})
	}
}
