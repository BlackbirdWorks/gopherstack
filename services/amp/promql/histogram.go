package promql

import (
	"math"
	"slices"
)

const (
	fnHistCount  = "histogram_count"
	fnHistSum    = "histogram_sum"
	fnHistAvg    = "histogram_avg"
	fnHistStddev = "histogram_stddev"
	fnHistStdvar = "histogram_stdvar"
)

// Span is a run of consecutive populated buckets starting Offset buckets after the previous span.
type Span struct {
	Offset int32  `json:"offset"`
	Length uint32 `json:"length"`
}

// FloatHistogram is a native histogram with absolute (not delta-encoded) bucket counts.
type FloatHistogram struct {
	PositiveSpans   []Span    `json:"ps,omitempty"`
	NegativeSpans   []Span    `json:"ns,omitempty"`
	PositiveBuckets []float64 `json:"pb,omitempty"`
	NegativeBuckets []float64 `json:"nb,omitempty"`
	ZeroThreshold   float64   `json:"zt,omitempty"`
	ZeroCount       float64   `json:"zc,omitempty"`
	Count           float64   `json:"c"`
	Sum             float64   `json:"s"`
	Schema          int32     `json:"schema"`
}

// Schema bounds accepted for exponential native histograms.
const (
	MinSchema = -4
	MaxSchema = 8
)

// HBucket is one materialised bucket; Lower is exclusive for positive buckets and Upper for negative ones.
type HBucket struct {
	Lower, Upper, Count float64
	Zero, Negative      bool
}

// Valid reports whether the span and bucket arrays agree and the schema is supported.
func (h *FloatHistogram) Valid() bool {
	return h.Schema >= MinSchema && h.Schema <= MaxSchema &&
		spanTotal(h.PositiveSpans) == len(h.PositiveBuckets) &&
		spanTotal(h.NegativeSpans) == len(h.NegativeBuckets)
}

func spanTotal(sp []Span) int {
	n := 0
	for _, s := range sp {
		n += int(s.Length)
	}

	return n
}

// bound is the upper boundary of bucket index i: 2^(i * 2^-schema).
func bound(i, schema int32) float64 {
	return math.Exp2(float64(i) * math.Exp2(-float64(schema)))
}

func expand(sp []Span, counts []float64, schema int32, neg bool) []HBucket {
	var (
		out []HBucket
		idx int32
		k   int
	)

	for _, s := range sp {
		idx += s.Offset

		for range s.Length {
			if k >= len(counts) {
				return out
			}

			lo, hi := bound(idx-1, schema), bound(idx, schema)
			if neg {
				lo, hi = -hi, -lo
			}

			out = append(out, HBucket{Lower: lo, Upper: hi, Count: counts[k], Negative: neg})
			idx++
			k++
		}
	}

	return out
}

// Buckets lists every bucket in increasing value order, the zero bucket included.
func (h *FloatHistogram) Buckets() []HBucket {
	neg := expand(h.NegativeSpans, h.NegativeBuckets, h.Schema, true)

	out := make([]HBucket, 0, len(neg)+len(h.PositiveBuckets)+1)
	for _, b := range slices.Backward(neg) {
		out = append(out, b)
	}

	out = append(out, HBucket{Lower: -h.ZeroThreshold, Upper: h.ZeroThreshold, Count: h.ZeroCount, Zero: true})

	return append(out, expand(h.PositiveSpans, h.PositiveBuckets, h.Schema, false)...)
}

func (h *FloatHistogram) accessor(fn string) float64 {
	switch fn {
	case fnHistCount:
		return h.Count
	case fnHistSum:
		return h.Sum
	case fnHistAvg:
		return h.Sum / h.Count
	case fnHistStdvar:
		return h.variance()
	default:
		return math.Sqrt(h.variance())
	}
}

func (h *FloatHistogram) variance() float64 {
	if h.Count == 0 {
		return math.NaN()
	}

	mean := h.Sum / h.Count
	v := 0.0

	for _, b := range h.Buckets() {
		m := 0.0
		if !b.Zero {
			m = math.Sqrt(b.Upper * b.Lower)
			if b.Upper < 0 {
				m = -m
			}
		}

		v += b.Count * (m - mean) * (m - mean)
	}

	return v / h.Count
}

// interp places a point at fraction f of the way through b, geometrically for regular buckets.
func interp(b HBucket, f float64) float64 {
	switch {
	case b.Zero || b.Lower == 0 || b.Upper == 0:
		return b.Lower + (b.Upper-b.Lower)*f
	case b.Lower > 0:
		return math.Exp2(math.Log2(b.Lower)*(1-f) + math.Log2(b.Upper)*f)
	default:
		return -math.Exp2(math.Log2(-b.Upper)*f + math.Log2(-b.Lower)*(1-f))
	}
}

// Quantile estimates the q-quantile like Prometheus' histogram_quantile on a native histogram.
func (h *FloatHistogram) Quantile(q float64) float64 {
	switch {
	case math.IsNaN(q) || h.Count == 0:
		return math.NaN()
	case q < 0:
		return math.Inf(-1)
	case q > 1:
		return math.Inf(1)
	}

	bs := h.Buckets()
	rank := q * h.Count

	var cum float64

	last := -1

	for i, b := range bs {
		if b.Count == 0 {
			continue
		}

		last = i

		if cum+b.Count >= rank {
			return interp(clipZero(bs, i), (rank-cum)/b.Count)
		}

		cum += b.Count
	}

	if last < 0 {
		return math.NaN()
	}

	return bs[last].Upper
}

// clipZero narrows the zero bucket to the side that holds observations when the other side is empty.
func clipZero(bs []HBucket, i int) HBucket {
	b := bs[i]
	if !b.Zero {
		return b
	}

	if allEmpty(bs[:i]) {
		b.Lower = 0
	}

	if allEmpty(bs[i+1:]) {
		b.Upper = 0
	}

	return b
}

func allEmpty(bs []HBucket) bool {
	for _, b := range bs {
		if b.Count != 0 {
			return false
		}
	}

	return true
}

// below is the interpolated number of observations <= x.
func (h *FloatHistogram) below(x float64) float64 {
	var cum float64

	for _, b := range h.Buckets() {
		switch {
		case x >= b.Upper:
			cum += b.Count
		case x > b.Lower:
			return cum + b.Count*fractionIn(b, x)
		default:
			return cum
		}
	}

	return cum
}

func fractionIn(b HBucket, x float64) float64 {
	if b.Zero || b.Lower <= 0 && b.Upper >= 0 {
		return (x - b.Lower) / (b.Upper - b.Lower)
	}

	if b.Lower > 0 {
		return (math.Log2(x) - math.Log2(b.Lower)) / (math.Log2(b.Upper) - math.Log2(b.Lower))
	}

	return 1 - (math.Log2(-x)-math.Log2(-b.Upper))/(math.Log2(-b.Lower)-math.Log2(-b.Upper))
}

// Fraction is the share of observations in [lower, upper].
func (h *FloatHistogram) Fraction(lower, upper float64) float64 {
	if h.Count == 0 || math.IsNaN(lower) || math.IsNaN(upper) {
		return math.NaN()
	}

	if lower >= upper {
		return 0
	}

	return (h.below(upper) - h.below(lower)) / h.Count
}

// isBareSelector reports an expression that is a plain instant selector.
func isBareSelector(x Expr) bool {
	_, ok := x.(*VectorSelector)

	return ok
}

// histArg evaluates arg and keeps native histogram samples when arg is a plain selector.
func (e *Engine) histArg(arg Expr, t int64, fn string) (Vector, error) {
	prev := e.histOK
	e.histOK = prev || isBareSelector(arg)

	defer func() { e.histOK = prev }()

	return e.instantArg(arg, t, fn)
}

func splitNative(v Vector) (Vector, Vector) {
	var native, floats Vector

	for _, s := range v {
		if s.H != nil {
			native = append(native, s)
		} else {
			floats = append(floats, s)
		}
	}

	return native, floats
}

// Selectors lists the matcher sets of every selector in expr.
func Selectors(expr Expr) [][]*Matcher {
	switch x := expr.(type) {
	case *VectorSelector:
		return [][]*Matcher{x.Matchers}
	case *MatrixSelector:
		return Selectors(x.VS)
	case *Subquery:
		return Selectors(x.Expr)
	case *Call:
		var out [][]*Matcher
		for _, a := range x.Args {
			out = append(out, Selectors(a)...)
		}

		return out
	case *Aggregation:
		out := Selectors(x.Expr)
		if x.Param != nil {
			out = append(out, Selectors(x.Param)...)
		}

		return out
	case *Binary:
		return append(Selectors(x.L), Selectors(x.R)...)
	case *Unary:
		return Selectors(x.X)
	}

	return nil
}
