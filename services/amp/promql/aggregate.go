package promql

import (
	"math"
	"slices"
	"strings"
)

type group struct {
	labels  Labels
	samples []VectorSample
}

func (e *Engine) evalAggregation(a *Aggregation, t int64) (Value, error) {
	vec, err := e.instantArg(a.Expr, t, a.Op)
	if err != nil {
		return nil, err
	}

	param := 0.0

	if a.Param != nil {
		if param, err = e.scalarArg(a.Param, t, a.Op); err != nil {
			return nil, err
		}
	}

	groups := map[string]*group{}
	order := []string{}

	for _, s := range vec {
		gl := s.Metric.without(NameLabel).pick(a.Grouping, !a.Without)
		k := Fingerprint(gl)

		g, ok := groups[k]
		if !ok {
			g = &group{labels: gl}
			groups[k] = g
			order = append(order, k)
		}

		g.samples = append(g.samples, s)
	}

	out := Vector{}

	for _, k := range order {
		g := groups[k]

		if a.Op == opTopK || a.Op == opBottomK {
			out = append(out, pickK(g.samples, int(param), a.Op == opTopK)...)

			continue
		}

		out = append(out, VectorSample{Metric: g.labels, T: t, V: reduce(a.Op, g.samples, param)})
	}

	sortVector(out)

	return out, nil
}

func pickK(samples []VectorSample, k int, top bool) Vector {
	if k <= 0 {
		return nil
	}

	sorted := slices.Clone(samples)
	slices.SortStableFunc(sorted, func(a, b VectorSample) int {
		switch {
		case a.V < b.V:
			if top {
				return 1
			}

			return -1
		case a.V > b.V:
			if top {
				return -1
			}

			return 1
		}

		return strings.Compare(Fingerprint(a.Metric), Fingerprint(b.Metric))
	})

	if k > len(sorted) {
		k = len(sorted)
	}

	return Vector(sorted[:k])
}

func reduce(op string, samples []VectorSample, param float64) float64 {
	vals := make([]float64, len(samples))
	for i, s := range samples {
		vals[i] = s.V
	}

	switch op {
	case "sum":
		return sum(vals)
	case "avg":
		return sum(vals) / float64(len(vals))
	case "min":
		return slices.Min(vals)
	case "max":
		return slices.Max(vals)
	case "count":
		return float64(len(vals))
	case "group":
		return 1
	case "stdvar":
		return variance(vals)
	case "stddev":
		return math.Sqrt(variance(vals))
	default:
		return quantile(param, vals)
	}
}

func variance(vals []float64) float64 {
	mean := sum(vals) / float64(len(vals))
	acc := 0.0

	for _, v := range vals {
		acc += (v - mean) * (v - mean)
	}

	return acc / float64(len(vals))
}

func quantile(q float64, vals []float64) float64 {
	switch {
	case isNaN(q):
		return math.NaN()
	case q < 0:
		return math.Inf(-1)
	case q > 1:
		return math.Inf(1)
	}

	sorted := slices.Clone(vals)
	slices.Sort(sorted)

	rank := q * float64(len(sorted)-1)
	lo := math.Floor(rank)
	hi := math.Ceil(rank)

	return sorted[int(lo)]*(1-(rank-lo)) + sorted[int(hi)]*(rank-lo)
}
