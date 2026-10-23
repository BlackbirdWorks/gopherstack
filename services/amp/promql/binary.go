package promql

import (
	"math"
	"slices"
)

func (e *Engine) evalBinary(b *Binary, t int64) (Value, error) {
	l, err := e.eval(b.L, t)
	if err != nil {
		return nil, err
	}

	r, err := e.eval(b.R, t)
	if err != nil {
		return nil, err
	}

	if isSetOp(b.Op) {
		lv, lok := l.(Vector)

		rv, rok := r.(Vector)
		if !lok || !rok {
			return nil, evalErr("set operator %q not allowed in binary scalar expression", b.Op)
		}

		return setOp(b, lv, rv, t), nil
	}

	ls, lScalar := l.(Scalar)
	rs, rScalar := r.(Scalar)

	switch {
	case lScalar && rScalar:
		return scalarBinary(b, ls, rs, t)
	case lScalar:
		return vectorScalar(b, r, ls.V, true)
	case rScalar:
		return vectorScalar(b, l, rs.V, false)
	}

	lv, lok := l.(Vector)

	rv, rok := r.(Vector)
	if !lok || !rok {
		return nil, evalErr("binary expression must contain only scalar and instant vector types")
	}

	return vectorVector(b, lv, rv, t)
}

func isSetOp(op string) bool { return op == opAnd || op == opOr || op == opUnless }

func isComparison(op string) bool {
	switch op {
	case "==", "!=", ">", "<", ">=", "<=":
		return true
	}

	return false
}

func applyOp(op string, l, r float64) (float64, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		return l / r, true
	case "%":
		return math.Mod(l, r), true
	case "^":
		return math.Pow(l, r), true
	case "==":
		return 1, l == r
	case "!=":
		return 1, l != r
	case ">":
		return 1, l > r
	case "<":
		return 1, l < r
	case ">=":
		return 1, l >= r
	case "<=":
		return 1, l <= r
	}

	return 0, false
}

func scalarBinary(b *Binary, l, r Scalar, t int64) (Value, error) {
	if isComparison(b.Op) && !b.ReturnBool {
		return nil, evalErr("comparisons between scalars must use BOOL modifier")
	}

	v, keep := applyOp(b.Op, l.V, r.V)
	if isComparison(b.Op) {
		v = boolValue(keep)
	}

	return Scalar{T: t, V: v}, nil
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

func vectorScalar(b *Binary, v Value, s float64, scalarLeft bool) (Value, error) {
	vec, ok := v.(Vector)
	if !ok {
		return nil, evalErr("binary expression must contain only scalar and instant vector types")
	}

	out := Vector{}

	for _, smp := range vec {
		l, r := smp.V, s
		if scalarLeft {
			l, r = s, smp.V
		}

		val, keep := applyOp(b.Op, l, r)

		switch {
		case isComparison(b.Op) && b.ReturnBool:
			out = append(out, VectorSample{Metric: smp.Metric.without(NameLabel), T: smp.T, V: boolValue(keep)})
		case isComparison(b.Op):
			if keep {
				out = append(out, VectorSample{Metric: smp.Metric, T: smp.T, V: smp.V})
			}
		default:
			out = append(out, VectorSample{Metric: smp.Metric.without(NameLabel), T: smp.T, V: val})
		}
	}

	return out, nil
}

func signature(b *Binary, l Labels) string {
	return Fingerprint(l.without(NameLabel).pick(b.MatchLabels, b.On))
}

func indexBySignature(b *Binary, v Vector) (map[string]VectorSample, bool) {
	idx := make(map[string]VectorSample, len(v))

	for _, s := range v {
		k := signature(b, s.Metric)
		if _, dup := idx[k]; dup {
			return nil, false
		}

		idx[k] = s
	}

	return idx, true
}

func vectorVector(b *Binary, l, r Vector, t int64) (Value, error) {
	many, one := l, r
	if b.Card == CardOneToMany {
		many, one = r, l
	}

	oneIdx, ok := indexBySignature(b, one)
	if !ok {
		return nil, evalErr("found duplicate series for the match group on the %s hand-side of the operation",
			oneSide(b))
	}

	if b.Card == CardOneToOne {
		if _, iok := indexBySignature(b, many); !iok {
			return nil, evalErr("found duplicate series for the match group on the left hand-side of the operation")
		}
	}

	out := Vector{}
	seen := map[string]bool{}

	for _, m := range many {
		o, found := oneIdx[signature(b, m.Metric)]
		if !found {
			continue
		}

		lv, rv := m, o
		if b.Card == CardOneToMany {
			lv, rv = o, m
		}

		smp, keep := combine(b, lv, rv, m.Metric, o.Metric, t)
		if !keep {
			continue
		}

		k := Fingerprint(smp.Metric)
		if seen[k] {
			return nil, evalErr("multiple matches for labels: grouping labels must ensure unique matches")
		}

		seen[k] = true

		out = append(out, smp)
	}

	sortVector(out)

	return out, nil
}

func oneSide(b *Binary) string {
	if b.Card == CardOneToMany {
		return "left"
	}

	return "right"
}

func combine(b *Binary, lv, rv VectorSample, manyM, oneM Labels, t int64) (VectorSample, bool) {
	val, keep := applyOp(b.Op, lv.V, rv.V)

	switch {
	case isComparison(b.Op) && b.ReturnBool:
		return VectorSample{Metric: resultMetric(b, manyM, oneM, true), T: t, V: boolValue(keep)}, true
	case isComparison(b.Op):
		return VectorSample{Metric: resultMetric(b, manyM, oneM, false), T: t, V: lv.V}, keep
	}

	return VectorSample{Metric: resultMetric(b, manyM, oneM, true), T: t, V: val}, true
}

func resultMetric(b *Binary, many, one Labels, dropName bool) Labels {
	metric := many.clone()
	if dropName {
		delete(metric, NameLabel)
	}

	if b.Card == CardOneToOne {
		if dropName && (b.On || len(b.MatchLabels) > 0) {
			metric = metric.pick(b.MatchLabels, b.On)
		}

		return metric
	}

	for _, n := range b.Include {
		if v, ok := one[n]; ok {
			metric[n] = v
		} else {
			delete(metric, n)
		}
	}

	return metric
}

func setOp(b *Binary, l, r Vector, t int64) Vector {
	rsigs := map[string]bool{}
	for _, s := range r {
		rsigs[signature(b, s.Metric)] = true
	}

	out := Vector{}

	switch b.Op {
	case opAnd:
		for _, s := range l {
			if rsigs[signature(b, s.Metric)] {
				out = append(out, s)
			}
		}
	case opUnless:
		for _, s := range l {
			if !rsigs[signature(b, s.Metric)] {
				out = append(out, s)
			}
		}
	default:
		lsigs := map[string]bool{}

		for _, s := range l {
			lsigs[signature(b, s.Metric)] = true
		}

		out = append(out, l...)

		for _, s := range r {
			if !lsigs[signature(b, s.Metric)] {
				out = append(out, VectorSample{Metric: s.Metric, T: t, V: s.V})
			}
		}
	}

	sortVector(out)

	return slices.Clip(out)
}
