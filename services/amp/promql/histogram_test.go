package promql_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func TestFloatHistogramQuantile(t *testing.T) {
	t.Parallel()

	zeroPos := &promql.FloatHistogram{
		ZeroThreshold: 0.5, ZeroCount: 2, Count: 4, Sum: 3,
		PositiveSpans: []promql.Span{{Offset: 1, Length: 1}}, PositiveBuckets: []float64{2},
	}
	neg := &promql.FloatHistogram{
		Count: 4, Sum: -6, NegativeSpans: []promql.Span{{Offset: 1, Length: 1}}, NegativeBuckets: []float64{4},
	}

	tests := []struct {
		h    *promql.FloatHistogram
		name string
		q    float64
		want float64
	}{
		{name: "zero bucket linear", h: zeroPos, q: 0.25, want: 0.25},
		{name: "positive geometric", h: zeroPos, q: 0.75, want: math.Sqrt2},
		{name: "top of last bucket", h: zeroPos, q: 1, want: 2},
		{name: "negative bucket", h: neg, q: 0.5, want: -math.Sqrt2},
		{name: "below zero", h: zeroPos, q: -1, want: math.Inf(-1)},
		{name: "above one", h: zeroPos, q: 2, want: math.Inf(1)},
		{name: "empty", h: &promql.FloatHistogram{}, q: 0.5, want: math.NaN()},
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
		h    promql.FloatHistogram
		want bool
	}{
		{
			name: "ok",
			h:    promql.FloatHistogram{PositiveSpans: []promql.Span{{Length: 2}}, PositiveBuckets: []float64{1, 1}},
			want: true,
		},
		{
			name: "mismatch",
			h:    promql.FloatHistogram{PositiveSpans: []promql.Span{{Length: 2}}, PositiveBuckets: []float64{1}},
		},
		{name: "schema high", h: promql.FloatHistogram{Schema: promql.MaxSchema + 1}},
		{name: "schema low", h: promql.FloatHistogram{Schema: promql.MinSchema - 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.h.Valid())
		})
	}
}
