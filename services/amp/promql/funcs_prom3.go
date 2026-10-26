package promql

import (
	"hash/fnv"
	"math"
	"slices"
	"strings"
)

// resolveAt turns start() and end() @ markers into the evaluated query's timestamps.
func resolveAt(expr Expr, start, end int64) {
	pick := func(marker string, cur *int64) *int64 {
		switch marker {
		case atStart:
			return &start
		case atEnd:
			return &end
		}

		return cur
	}

	switch x := expr.(type) {
	case *VectorSelector:
		x.At = pick(x.AtMarker, x.At)
	case *MatrixSelector:
		resolveAt(x.VS, start, end)
	case *Subquery:
		x.At = pick(x.AtMarker, x.At)
		resolveAt(x.Expr, start, end)
	case *Call:
		for _, a := range x.Args {
			resolveAt(a, start, end)
		}
	case *Aggregation:
		resolveAt(x.Expr, start, end)

		if x.Param != nil {
			resolveAt(x.Param, start, end)
		}
	case *Binary:
		resolveAt(x.L, start, end)
		resolveAt(x.R, start, end)
	case *Unary:
		resolveAt(x.X, start, end)
	}
}

func (e *Engine) callDoubleExponentialSmoothing(c *Call, t int64) (Value, error) {
	sf, err := e.scalarArg(c.Args[1], t, c.Func)
	if err != nil {
		return nil, err
	}

	tf, err := e.scalarArg(c.Args[2], t, c.Func)
	if err != nil {
		return nil, err
	}

	if !(sf > 0 && sf < 1) {
		return nil, evalErr("invalid smoothing factor. Expected: 0 < sf < 1, got: %v", sf)
	}

	if !(tf > 0 && tf < 1) {
		return nil, evalErr("invalid trend factor. Expected: 0 < tf < 1, got: %v", tf)
	}

	fn := func(s []Sample, _, _ int64) (float64, bool) {
		if len(s) < minSamples {
			return 0, false
		}

		return smooth(s, sf, tf), true
	}

	return e.rangeOver(c.Args[0], t, c.Func, fn)
}

// smooth is Holt-Winters double exponential smoothing as Prometheus computes it.
func smooth(s []Sample, sf, tf float64) float64 {
	var s0 float64

	s1 := s[0].V
	b := s[1].V - s[0].V

	for i := 1; i < len(s); i++ {
		if i > 1 {
			b = tf*(s1-s0) + (1-tf)*b
		}

		s0, s1 = s1, sf*s[i].V+(1-sf)*(s1+b)
	}

	return s1
}

func (e *Engine) callSortByLabel(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	labels := make([]string, 0, len(c.Args)-1)

	for _, a := range c.Args[1:] {
		l, serr := e.stringArg(a, t, c.Func)
		if serr != nil {
			return nil, serr
		}

		labels = append(labels, l)
	}

	desc := c.Func == fnSortByLabelDesc
	out := slices.Clone(vec)

	slices.SortStableFunc(out, func(a, b VectorSample) int {
		for _, l := range labels {
			if cmp := naturalCompare(a.Metric[l], b.Metric[l]); cmp != 0 {
				if desc {
					return -cmp
				}

				return cmp
			}
		}

		return strings.Compare(Fingerprint(a.Metric), Fingerprint(b.Metric))
	})

	return out, nil
}

// naturalCompare orders strings with embedded digit runs by numeric value.
func naturalCompare(a, b string) int {
	for a != "" && b != "" {
		ca, cb := chunk(a), chunk(b)
		a, b = a[len(ca):], b[len(cb):]

		if ca == cb {
			continue
		}

		if isDigit(ca[0]) && isDigit(cb[0]) {
			na, nb := strings.TrimLeft(ca, "0"), strings.TrimLeft(cb, "0")
			if len(na) != len(nb) {
				return len(na) - len(nb)
			}

			if cmp := strings.Compare(na, nb); cmp != 0 {
				return cmp
			}
		}

		if cmp := strings.Compare(ca, cb); cmp != 0 {
			return cmp
		}
	}

	return len(a) - len(b)
}

// chunk is the leading run of digits or non-digits.
func chunk(s string) string {
	digit := isDigit(s[0])

	for i := 1; i < len(s); i++ {
		if isDigit(s[i]) != digit {
			return s[:i]
		}
	}

	return s
}

// callNativeHistogram applies the native-histogram accessors; Prometheus ignores float samples, the only kind stored.
func (e *Engine) callNativeHistogram(c *Call, t int64) (Value, error) {
	if _, err := e.instantArg(c.Args[0], t, c.Func); err != nil {
		return nil, err
	}

	return Vector{}, nil
}

func (e *Engine) callHistogramFraction(c *Call, t int64) (Value, error) {
	lower, err := e.scalarArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	upper, err := e.scalarArg(c.Args[1], t, c.Func)
	if err != nil {
		return nil, err
	}

	vec, err := e.instantArg(c.Args[2], t, c.Func)
	if err != nil {
		return nil, err
	}

	groups, order := groupBuckets(vec)

	out := Vector{}

	for _, k := range order {
		g := groups[k]
		out = append(out, VectorSample{Metric: g.labels, T: t, V: bucketFraction(lower, upper, g.buckets)})
	}

	sortVector(out)

	return out, nil
}

// bucketFraction is the share of observations in [lower, upper] of a classic histogram, interpolating linearly.
func bucketFraction(lower, upper float64, bs []bucket) float64 {
	slices.SortFunc(bs, func(a, b bucket) int {
		switch {
		case a.upper < b.upper:
			return -1
		case a.upper > b.upper:
			return 1
		}

		return 0
	})

	if len(bs) < minSamples || !math.IsInf(bs[len(bs)-1].upper, 1) {
		return math.NaN()
	}

	for i := 1; i < len(bs); i++ {
		bs[i].count = math.Max(bs[i].count, bs[i-1].count)
	}

	total := bs[len(bs)-1].count
	if total == 0 || math.IsNaN(lower) || math.IsNaN(upper) {
		return math.NaN()
	}

	if lower >= upper {
		return 0
	}

	return (rankOf(bs, upper) - rankOf(bs, lower)) / total
}

// rankOf is the cumulative observation count at v, interpolated inside its bucket.
func rankOf(bs []bucket, v float64) float64 {
	prev := 0.0
	lowerBound := math.Inf(-1)

	for i, b := range bs {
		if i == 0 && b.upper > 0 {
			lowerBound = 0
		}

		if v <= lowerBound {
			return prev
		}

		if v < b.upper {
			if math.IsInf(lowerBound, 0) || math.IsInf(b.upper, 0) {
				return prev
			}

			return prev + (b.count-prev)*(v-lowerBound)/(b.upper-lowerBound)
		}

		prev, lowerBound = b.count, b.upper
	}

	return prev
}

// limitSamples keeps the first k samples (limitk) or a deterministic ratio of them (limit_ratio) per group.
func limitSamples(op string, samples []VectorSample, param float64) Vector {
	if op == opLimitK {
		k := int(min(max(param, 0), float64(len(samples))))

		return slices.Clone(samples[:k])
	}

	ratio := math.Max(-1, math.Min(1, param))
	out := Vector{}

	for _, s := range samples {
		h := fnv.New64a()
		_, _ = h.Write([]byte(Fingerprint(s.Metric)))
		offset := float64(h.Sum64()) / float64(math.MaxUint64)

		if (ratio >= 0 && offset < ratio) || (ratio < 0 && offset >= 1+ratio) {
			out = append(out, s)
		}
	}

	return out
}
