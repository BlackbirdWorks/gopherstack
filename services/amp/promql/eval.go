package promql

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// DefaultLookback is how far back an instant selector looks for a sample.
const DefaultLookback = 5 * time.Minute

const (
	nameScalar = "scalar"
	nameVector = "vector"
	opAnd      = "and"
	opOr       = "or"
	opUnless   = "unless"
	opTopK     = "topk"
	opBottomK  = "bottomk"
)

// MaxRangeSteps bounds a range query's resolution like Prometheus.
const MaxRangeSteps = 11000

// ErrEval marks an evaluation failure.
var ErrEval = errors.New("execution error")

func evalErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrEval, fmt.Sprintf(format, args...))
}

// Value is the result of evaluating an expression.
type Value interface{ valueType() string }

// Scalar is a single number.
type Scalar struct {
	T int64
	V float64
}

// VectorSample is one instant sample of a labelled series.
type VectorSample struct {
	Metric Labels
	T      int64
	V      float64
}

// Vector is a set of instant samples.
type Vector []VectorSample

// Matrix is a set of series with ranges of samples.
type Matrix []Series

// StringValue is a string result.
type StringValue struct {
	V string
	T int64
}

func (Scalar) valueType() string      { return nameScalar }
func (Vector) valueType() string      { return nameVector }
func (Matrix) valueType() string      { return "matrix" }
func (StringValue) valueType() string { return "string" }

// ResultType names a value's Prometheus resultType.
func ResultType(v Value) string { return v.valueType() }

// Engine evaluates expressions against a Storage.
type Engine struct {
	Store    Storage
	Lookback time.Duration
}

func (e *Engine) lookback() time.Duration {
	if e.Lookback > 0 {
		return e.Lookback
	}

	return DefaultLookback
}

// Instant evaluates src at ts.
func (e *Engine) Instant(src string, ts time.Time) (Value, error) {
	expr, err := Parse(src)
	if err != nil {
		return nil, err
	}

	t := ts.UnixMilli()

	v, err := e.eval(expr, t)
	if err != nil {
		return nil, err
	}

	if m, ok := v.(Matrix); ok {
		return m, nil
	}

	return v, nil
}

// Range evaluates src at every step in [start, end].
func (e *Engine) Range(src string, start, end time.Time, step time.Duration) (Matrix, error) {
	if step <= 0 {
		return nil, evalErr("zero or negative query resolution step widths are not accepted")
	}

	if end.Before(start) {
		return nil, evalErr("end timestamp must not be before start time")
	}

	if int64(end.Sub(start)/step) > MaxRangeSteps {
		return nil, evalErr("exceeded maximum resolution of %d points per timeseries", MaxRangeSteps)
	}

	expr, err := Parse(src)
	if err != nil {
		return nil, err
	}

	if _, isMatrix := expr.(*MatrixSelector); isMatrix {
		return nil, evalErr(
			"invalid expression type \"range vector\" for range query, must be Scalar or instant Vector",
		)
	}

	acc := map[string]*Series{}
	order := []string{}

	for t := start; !t.After(end); t = t.Add(step) {
		v, ierr := e.eval(expr, t.UnixMilli())
		if ierr != nil {
			return nil, ierr
		}

		switch x := v.(type) {
		case Scalar:
			addPoint(acc, &order, Labels{}, Sample(x))
		case Vector:
			for _, s := range x {
				addPoint(acc, &order, s.Metric, Sample{T: s.T, V: s.V})
			}
		default:
			return nil, evalErr("invalid expression type %q for range query", v.valueType())
		}
	}

	out := make(Matrix, 0, len(order))
	for _, k := range order {
		out = append(out, *acc[k])
	}

	sortSeries(out)

	return out, nil
}

func addPoint(acc map[string]*Series, order *[]string, l Labels, s Sample) {
	k := Fingerprint(l)

	ser, ok := acc[k]
	if !ok {
		ser = &Series{Labels: l}
		acc[k] = ser
		*order = append(*order, k)
	}

	ser.Samples = append(ser.Samples, s)
}

func sortSeries(m Matrix) {
	slices.SortFunc(m, func(a, b Series) int { return strings.Compare(Fingerprint(a.Labels), Fingerprint(b.Labels)) })
}

func (e *Engine) eval(expr Expr, t int64) (Value, error) {
	switch x := expr.(type) {
	case *NumberLit:
		return Scalar{T: t, V: x.V}, nil
	case *StringLit:
		return StringValue{T: t, V: x.V}, nil
	case *VectorSelector:
		return e.evalVector(x, t), nil
	case *MatrixSelector:
		return e.evalMatrix(x, t), nil
	case *Unary:
		return e.evalUnary(x, t)
	case *Call:
		return e.evalCall(x, t)
	case *Aggregation:
		return e.evalAggregation(x, t)
	case *Binary:
		return e.evalBinary(x, t)
	}

	return nil, evalErr("unsupported expression %T", expr)
}

func (e *Engine) evalVector(vs *VectorSelector, t int64) Vector {
	minT, maxT := selectorRange(vs, t, e.lookback())
	series := e.Store.Select(vs.Matchers, minT, maxT)
	out := make(Vector, 0, len(series))

	for _, s := range series {
		if p, ok := lastAtOrBefore(s.Samples, maxT, minT); ok {
			out = append(out, VectorSample{Metric: s.Labels, T: t, V: p.V})
		}
	}

	sortVector(out)

	return out
}

// lastAtOrBefore returns the newest sample in (minT, maxT].
func lastAtOrBefore(samples []Sample, maxT, minT int64) (Sample, bool) {
	for i, p := range slices.Backward(samples) {
		if p.T > maxT {
			continue
		}

		if p.T > minT {
			return samples[i], true
		}

		break
	}

	return Sample{}, false
}

func (e *Engine) evalMatrix(ms *MatrixSelector, t int64) Matrix {
	minT, maxT := selectorRange(ms.VS, t, ms.Range)
	series := e.Store.Select(ms.VS.Matchers, minT, maxT)
	out := make(Matrix, 0, len(series))

	for _, s := range series {
		var win []Sample

		for _, p := range s.Samples {
			if p.T > minT && p.T <= maxT {
				win = append(win, p)
			}
		}

		if len(win) > 0 {
			out = append(out, Series{Labels: s.Labels, Samples: win})
		}
	}

	sortSeries(out)

	return out
}

func sortVector(v Vector) {
	slices.SortFunc(v, func(a, b VectorSample) int {
		return strings.Compare(Fingerprint(a.Metric), Fingerprint(b.Metric))
	})
}

func (e *Engine) evalUnary(u *Unary, t int64) (Value, error) {
	v, err := e.eval(u.X, t)
	if err != nil {
		return nil, err
	}

	switch x := v.(type) {
	case Scalar:
		return Scalar{T: x.T, V: -x.V}, nil
	case Vector:
		out := make(Vector, len(x))
		for i, s := range x {
			out[i] = VectorSample{Metric: s.Metric.without(NameLabel), T: s.T, V: -s.V}
		}

		return out, nil
	}

	return nil, evalErr("unary expression only allowed on expressions of type scalar or instant vector, got %q",
		v.valueType())
}

func isNaN(f float64) bool { return math.IsNaN(f) }
