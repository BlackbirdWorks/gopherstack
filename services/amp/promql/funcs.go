package promql

import (
	"math"
	"slices"
)

type funcSpec struct {
	minArgs, maxArgs int
}

var functions = map[string]funcSpec{ //nolint:gochecknoglobals // immutable lookup
	"rate": {1, 1}, "irate": {1, 1}, "increase": {1, 1}, "delta": {1, 1}, "idelta": {1, 1},
	"avg_over_time": {1, 1}, "sum_over_time": {1, 1}, "min_over_time": {1, 1}, "max_over_time": {1, 1},
	"count_over_time": {1, 1}, "last_over_time": {1, 1}, "changes": {1, 1}, "resets": {1, 1},
	"abs": {1, 1}, "ceil": {1, 1}, "floor": {1, 1}, "sqrt": {1, 1}, "exp": {1, 1}, "ln": {1, 1},
	"log2": {1, 1}, "log10": {1, 1}, "round": {1, 2}, "clamp": {3, 3}, "clamp_min": {2, 2}, "clamp_max": {2, 2},
	"absent": {1, 1}, "time": {0, 0}, nameVector: {1, 1}, nameScalar: {1, 1}, "sort": {1, 1}, fnSortDesc: {1, 1},
}

const (
	msPerSecond = 1000.0
	roundHalf   = 0.5
	half        = 2
	fnSortDesc  = "sort_desc"
	minSamples  = 2
)

func (e *Engine) evalCall(c *Call, t int64) (Value, error) {
	switch c.Func {
	case "time":
		return Scalar{T: t, V: float64(t) / msPerSecond}, nil
	case nameVector:
		return e.callVector(c, t)
	case nameScalar:
		return e.callScalar(c, t)
	case "absent":
		return e.callAbsent(c, t)
	case "sort", fnSortDesc:
		return e.callSort(c, t)
	}

	if rangeFuncs[c.Func] != nil {
		return e.callRange(c, t)
	}

	return e.callMath(c, t)
}

func (e *Engine) instantArg(arg Expr, t int64, fn string) (Vector, error) {
	v, err := e.eval(arg, t)
	if err != nil {
		return nil, err
	}

	vec, ok := v.(Vector)
	if !ok {
		return nil, evalErr("expected type instant vector in call to function %q, got %s", fn, v.valueType())
	}

	return vec, nil
}

func (e *Engine) scalarArg(arg Expr, t int64, fn string) (float64, error) {
	v, err := e.eval(arg, t)
	if err != nil {
		return 0, err
	}

	s, ok := v.(Scalar)
	if !ok {
		return 0, evalErr("expected type scalar in call to function %q, got %s", fn, v.valueType())
	}

	return s.V, nil
}

func (e *Engine) callVector(c *Call, t int64) (Value, error) {
	f, err := e.scalarArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	return Vector{{Metric: Labels{}, T: t, V: f}}, nil
}

func (e *Engine) callScalar(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	if len(vec) != 1 {
		return Scalar{T: t, V: math.NaN()}, nil
	}

	return Scalar{T: t, V: vec[0].V}, nil
}

func (e *Engine) callAbsent(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	if len(vec) > 0 {
		return Vector{}, nil
	}

	lbls := Labels{}

	if vs, ok := c.Args[0].(*VectorSelector); ok {
		for _, m := range vs.Matchers {
			if m.Type == MatchEqual && m.Name != NameLabel {
				lbls[m.Name] = m.Value
			}
		}
	}

	return Vector{{Metric: lbls, T: t, V: 1}}, nil
}

func (e *Engine) callSort(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	out := slices.Clone(vec)
	desc := c.Func == fnSortDesc

	slices.SortStableFunc(out, func(a, b VectorSample) int {
		switch {
		case a.V < b.V:
			if desc {
				return 1
			}

			return -1
		case a.V > b.V:
			if desc {
				return -1
			}

			return 1
		}

		return 0
	})

	return out, nil
}

func (e *Engine) callMath(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	extra := make([]float64, 0, len(c.Args)-1)

	for _, a := range c.Args[1:] {
		f, ierr := e.scalarArg(a, t, c.Func)
		if ierr != nil {
			return nil, ierr
		}

		extra = append(extra, f)
	}

	fn := mathFunc(c.Func, extra)
	out := make(Vector, len(vec))

	for i, s := range vec {
		out[i] = VectorSample{Metric: s.Metric.without(NameLabel), T: s.T, V: fn(s.V)}
	}

	return out, nil
}

func mathFunc(name string, extra []float64) func(float64) float64 {
	if f := plainMath(name); f != nil {
		return f
	}

	switch name {
	case "round":
		to := 1.0
		if len(extra) > 0 && extra[0] != 0 {
			to = extra[0]
		}

		return func(v float64) float64 { return math.Floor(v/to+roundHalf) * to }
	case "clamp_min":
		return func(v float64) float64 { return math.Max(v, extra[0]) }
	case "clamp_max":
		return func(v float64) float64 { return math.Min(v, extra[0]) }
	default:
		return func(v float64) float64 {
			if extra[0] > extra[1] {
				return math.NaN()
			}

			return math.Max(extra[0], math.Min(extra[1], v))
		}
	}
}

func plainMath(name string) func(float64) float64 {
	switch name {
	case "abs":
		return math.Abs
	case "ceil":
		return math.Ceil
	case "floor":
		return math.Floor
	case "sqrt":
		return math.Sqrt
	case "exp":
		return math.Exp
	case "ln":
		return math.Log
	case "log2":
		return math.Log2
	case "log10":
		return math.Log10
	}

	return nil
}

type rangeFn func(samples []Sample, start, end int64) (float64, bool)

var rangeFuncs = map[string]rangeFn{ //nolint:gochecknoglobals // immutable lookup
	"rate":            func(s []Sample, a, b int64) (float64, bool) { return extrapolatedRate(s, a, b, true, true) },
	"increase":        func(s []Sample, a, b int64) (float64, bool) { return extrapolatedRate(s, a, b, true, false) },
	"delta":           func(s []Sample, a, b int64) (float64, bool) { return extrapolatedRate(s, a, b, false, false) },
	"irate":           func(s []Sample, _, _ int64) (float64, bool) { return instantRate(s, true) },
	"idelta":          func(s []Sample, _, _ int64) (float64, bool) { return instantRate(s, false) },
	"avg_over_time":   overTime(func(v []float64) float64 { return sum(v) / float64(len(v)) }),
	"sum_over_time":   overTime(sum),
	"min_over_time":   overTime(slices.Min[[]float64]),
	"max_over_time":   overTime(slices.Max[[]float64]),
	"count_over_time": overTime(func(v []float64) float64 { return float64(len(v)) }),
	"last_over_time":  overTime(func(v []float64) float64 { return v[len(v)-1] }),
	"changes":         func(s []Sample, _, _ int64) (float64, bool) { return countTransitions(s, false), true },
	"resets":          func(s []Sample, _, _ int64) (float64, bool) { return countTransitions(s, true), true },
}

func sum(v []float64) float64 {
	total := 0.0
	for _, f := range v {
		total += f
	}

	return total
}

func overTime(agg func([]float64) float64) rangeFn {
	return func(s []Sample, _, _ int64) (float64, bool) {
		if len(s) == 0 {
			return 0, false
		}

		vals := make([]float64, len(s))
		for i, p := range s {
			vals[i] = p.V
		}

		return agg(vals), true
	}
}

func countTransitions(s []Sample, resetsOnly bool) float64 {
	n := 0.0

	for i := 1; i < len(s); i++ {
		switch {
		case resetsOnly && s[i].V < s[i-1].V:
			n++
		case !resetsOnly && s[i].V != s[i-1].V:
			n++
		}
	}

	return n
}

func instantRate(s []Sample, isRate bool) (float64, bool) {
	if len(s) < minSamples {
		return 0, false
	}

	last, prev := s[len(s)-1], s[len(s)-2]

	diff := last.V - prev.V
	if isRate && last.V < prev.V {
		diff = last.V
	}

	dt := float64(last.T-prev.T) / msPerSecond
	if dt == 0 {
		return 0, false
	}

	if isRate {
		return diff / dt, true
	}

	return diff, true
}

const extrapolationSlack = 1.1

func extrapolatedRate(samples []Sample, rangeStart, rangeEnd int64, isCounter, isRate bool) (float64, bool) {
	if len(samples) < minSamples {
		return 0, false
	}

	first, last := samples[0], samples[len(samples)-1]
	result := last.V - first.V

	if isCounter {
		prev := first.V

		for _, s := range samples[1:] {
			if s.V < prev {
				result += prev
			}

			prev = s.V
		}
	}

	toStart := float64(first.T-rangeStart) / msPerSecond
	toEnd := float64(rangeEnd-last.T) / msPerSecond
	sampled := float64(last.T-first.T) / msPerSecond
	avgGap := sampled / float64(len(samples)-1)
	threshold := avgGap * extrapolationSlack

	if toStart >= threshold {
		toStart = avgGap / half
	}

	if isCounter && result > 0 && first.V >= 0 {
		if toZero := sampled * (first.V / result); toZero < toStart {
			toStart = toZero
		}
	}

	if toEnd >= threshold {
		toEnd = avgGap / half
	}

	factor := (sampled + toStart + toEnd) / sampled
	if isRate {
		factor /= float64(rangeEnd-rangeStart) / msPerSecond
	}

	return result * factor, true
}

func (e *Engine) callRange(c *Call, t int64) (Value, error) {
	ms, ok := c.Args[0].(*MatrixSelector)
	if !ok {
		return nil, evalErr("expected type range vector in call to function %q", c.Func)
	}

	fn := rangeFuncs[c.Func]
	start, end := selectorRange(ms.VS, t, ms.Range)
	out := Vector{}

	for _, s := range e.evalMatrix(ms, t) {
		if v, iok := fn(s.Samples, start, end); iok {
			out = append(out, VectorSample{Metric: s.Labels.without(NameLabel), T: t, V: v})
		}
	}

	sortVector(out)

	return out, nil
}
