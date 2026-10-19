package athena

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"

	sqlitedriver "modernc.org/sqlite"
)

// aggState is one running aggregate; step ignores NULL inputs unless nullsToo.
type aggState struct {
	step  func(args []driver.Value) error
	final func() driver.Value
}

func (a *aggState) Step(_ *fnCtx, args []driver.Value) error { return a.step(args) }

func (a *aggState) WindowInverse(_ *fnCtx, _ []driver.Value) error { return errNoWindowInverse }

func (a *aggState) WindowValue(_ *fnCtx) (driver.Value, error) { return a.final(), nil }

func (a *aggState) Final(_ *fnCtx) {}

var errNoWindowInverse = errors.New("this aggregate cannot be used as a window function")

func registerAgg(name string, nargs int32, newState func() *aggState) {
	sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
		NArgs: nargs,
		MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) {
			return newState(), nil
		},
	})
}

func numericArg(v driver.Value) (float64, bool) { return toFloatArg(v) }

func newCollector(onValue func(v driver.Value), result func() driver.Value) *aggState {
	return &aggState{
		step: func(args []driver.Value) error {
			if args[0] != nil {
				onValue(args[0])
			}

			return nil
		},
		final: result,
	}
}

func approxPercentile() *aggState {
	var (
		vals []float64
		p    float64
	)

	return &aggState{
		step: func(args []driver.Value) error {
			if f, ok := numericArg(args[0]); ok {
				vals = append(vals, f)
			}

			if f, ok := numericArg(args[1]); ok {
				p = f
			}

			return nil
		},
		final: func() driver.Value {
			if len(vals) == 0 {
				return sqlNull()
			}

			sort.Float64s(vals)

			idx := int(math.Ceil(p*float64(len(vals)))) - 1

			return vals[max(0, min(idx, len(vals)-1))]
		},
	}
}

func arbitrary() *aggState {
	var first driver.Value

	return newCollector(func(v driver.Value) {
		if first == nil {
			first = v
		}
	}, func() driver.Value { return first })
}

func maxMinBy(wantMax bool) *aggState {
	var (
		best driver.Value
		key  driver.Value
	)

	return &aggState{
		step: func(args []driver.Value) error {
			if args[1] == nil {
				return nil
			}

			cmp := 0
			if key != nil {
				cmp = compareValues(args[1], key)
			}

			if key == nil || (wantMax && cmp > 0) || (!wantMax && cmp < 0) {
				best, key = args[0], args[1]
			}

			return nil
		},
		final: func() driver.Value { return best },
	}
}

func compareValues(a, b driver.Value) int {
	af, aok := toFloatArg(a)
	bf, bok := toFloatArg(b)

	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}

	as, _ := asString(a)
	bs, _ := asString(b)

	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func jsonElem(v driver.Value) any {
	if s, ok := v.(string); ok {
		var parsed any
		if t := strings.TrimPrefix(s, containerMark); len(t) > 0 && (t[0] == '[' || t[0] == '{') &&
			json.Unmarshal([]byte(t), &parsed) == nil {
			return parsed
		}
	}

	if b, ok := v.([]byte); ok {
		return string(b)
	}

	return v
}

func arrayAgg() *aggState {
	out := []any{}

	return &aggState{
		step: func(args []driver.Value) error {
			out = append(out, jsonElem(args[0]))

			return nil
		},
		final: func() driver.Value { return jsonText(out) },
	}
}

func mapAgg() *aggState {
	m := map[string]any{}

	return &aggState{
		step: func(args []driver.Value) error {
			if k, ok := asKey(args[0]); ok {
				m[k] = jsonElem(args[1])
			}

			return nil
		},
		final: func() driver.Value { return jsonText(m) },
	}
}

func histogram() *aggState {
	m := map[string]int64{}

	return newCollector(func(v driver.Value) {
		k, _ := asKey(v)
		m[k]++
	}, func() driver.Value { return jsonText(m) })
}

func countIf() *aggState {
	var n int64

	return &aggState{
		step: func(args []driver.Value) error {
			if v, ok := toInt(args[0]); ok && v != 0 {
				n++
			}

			return nil
		},
		final: func() driver.Value { return n },
	}
}

func boolAgg(and bool) *aggState {
	var (
		seen bool
		acc  = and
	)

	return newCollector(func(v driver.Value) {
		b, _ := toInt(v)
		seen = true

		if and {
			acc = acc && b != 0
		} else {
			acc = acc || b != 0
		}
	}, func() driver.Value {
		if !seen {
			return sqlNull()
		}

		return boolInt(acc)
	})
}

// moments tracks count, mean and M2 (Welford) for variance family aggregates.
func moments(final func(n int64, m2 float64) driver.Value) *aggState {
	var (
		n    int64
		mean float64
		m2   float64
	)

	return newCollector(func(v driver.Value) {
		f, ok := numericArg(v)
		if !ok {
			return
		}

		n++
		d := f - mean
		mean += d / float64(n)
		m2 += d * (f - mean)
	}, func() driver.Value { return final(n, m2) })
}

func varianceOf(sample, root bool) *aggState {
	return moments(func(n int64, m2 float64) driver.Value {
		div := float64(n)
		if sample {
			div--
		}

		if n == 0 || div <= 0 {
			return sqlNull()
		}

		v := m2 / div
		if root {
			return math.Sqrt(v)
		}

		return v
	})
}

func geometricMean() *aggState {
	var (
		n   int64
		sum float64
	)

	return newCollector(func(v driver.Value) {
		if f, ok := numericArg(v); ok {
			n++
			sum += math.Log(f)
		}
	}, func() driver.Value {
		if n == 0 {
			return sqlNull()
		}

		return math.Exp(sum / float64(n))
	})
}

// pairStats accumulates x/y sums for corr and covariance.
func pairStats(final func(n, sx, sy, sxy, sxx, syy float64) driver.Value) *aggState {
	var n, sx, sy, sxy, sxx, syy float64

	return &aggState{
		step: func(args []driver.Value) error {
			y, yok := numericArg(args[0])
			x, xok := numericArg(args[1])

			if yok && xok {
				n++
				sx += x
				sy += y
				sxy += x * y
				sxx += x * x
				syy += y * y
			}

			return nil
		},
		final: func() driver.Value { return final(n, sx, sy, sxy, sxx, syy) },
	}
}

func covariance(sample bool) *aggState {
	return pairStats(func(n, sx, sy, sxy, _, _ float64) driver.Value {
		div := n
		if sample {
			div--
		}

		if n == 0 || div <= 0 {
			return sqlNull()
		}

		return (sxy - sx*sy/n) / div
	})
}

func correlation() *aggState {
	return pairStats(func(n, sx, sy, sxy, sxx, syy float64) driver.Value {
		den := math.Sqrt((sxx - sx*sx/n) * (syy - sy*sy/n))
		if n == 0 || den == 0 {
			return sqlNull()
		}

		return (sxy - sx*sy/n) / den
	})
}

func registerAggregates() {
	registerAgg("approx_percentile", argsTwo, approxPercentile)
	registerAgg("arbitrary", 1, arbitrary)
	registerAgg("any_value", 1, arbitrary)
	registerAgg("max_by", argsTwo, func() *aggState { return maxMinBy(true) })
	registerAgg("min_by", argsTwo, func() *aggState { return maxMinBy(false) })
	registerAgg("array_agg", 1, arrayAgg)
	registerAgg("map_agg", argsTwo, mapAgg)
	registerAgg("histogram", 1, histogram)
	registerAgg("count_if", 1, countIf)
	registerAgg("bool_and", 1, func() *aggState { return boolAgg(true) })
	registerAgg("every", 1, func() *aggState { return boolAgg(true) })
	registerAgg("bool_or", 1, func() *aggState { return boolAgg(false) })
	registerAgg("stddev", 1, func() *aggState { return varianceOf(true, true) })
	registerAgg("stddev_samp", 1, func() *aggState { return varianceOf(true, true) })
	registerAgg("stddev_pop", 1, func() *aggState { return varianceOf(false, true) })
	registerAgg("variance", 1, func() *aggState { return varianceOf(true, false) })
	registerAgg("var_samp", 1, func() *aggState { return varianceOf(true, false) })
	registerAgg("var_pop", 1, func() *aggState { return varianceOf(false, false) })
	registerAgg("geometric_mean", 1, geometricMean)
	registerAgg("covar_samp", argsTwo, func() *aggState { return covariance(true) })
	registerAgg("covar_pop", argsTwo, func() *aggState { return covariance(false) })
	registerAgg("corr", argsTwo, correlation)
}
