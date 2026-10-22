package promql

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const labelReplaceArgs = 4

var dateFuncs = map[string]func(time.Time) float64{ //nolint:gochecknoglobals // immutable lookup
	"minute":        func(t time.Time) float64 { return float64(t.Minute()) },
	"hour":          func(t time.Time) float64 { return float64(t.Hour()) },
	"month":         func(t time.Time) float64 { return float64(t.Month()) },
	"year":          func(t time.Time) float64 { return float64(t.Year()) },
	"day_of_month":  func(t time.Time) float64 { return float64(t.Day()) },
	"day_of_week":   func(t time.Time) float64 { return float64(t.Weekday()) },
	"day_of_year":   func(t time.Time) float64 { return float64(t.YearDay()) },
	"days_in_month": func(t time.Time) float64 { return float64(daysIn(t)) },
}

func daysIn(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// callExtra dispatches the functions outside the core set; ok is false when c is not one of them.
func (e *Engine) callExtra(c *Call, t int64) (Value, bool, error) {
	var (
		v   Value
		err error
	)

	switch c.Func {
	case "label_replace":
		v, err = e.callLabelReplace(c, t)
	case "label_join":
		v, err = e.callLabelJoin(c, t)
	case "histogram_quantile":
		v, err = e.callHistogramQuantile(c, t)
	case "predict_linear":
		v, err = e.callPredictLinear(c, t)
	case "quantile_over_time":
		v, err = e.callQuantileOverTime(c, t)
	case "timestamp":
		v, err = e.callTimestamp(c, t)
	default:
		f, ok := dateFuncs[c.Func]
		if !ok {
			return nil, false, nil
		}

		v, err = e.callDate(c, t, f)
	}

	return v, true, err
}

func (e *Engine) stringArg(arg Expr, t int64, fn string) (string, error) {
	v, err := e.eval(arg, t)
	if err != nil {
		return "", err
	}

	s, ok := v.(StringValue)
	if !ok {
		return "", evalErr("expected type string in call to function %q, got %s", fn, v.valueType())
	}

	return s.V, nil
}

func validLabelName(s string) bool {
	if s == "" {
		return false
	}

	for i := range len(s) {
		ch := s[i]
		if ch != '_' && (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (i == 0 || !isDigit(ch)) {
			return false
		}
	}

	return true
}

func (e *Engine) callLabelReplace(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	strs := make([]string, labelReplaceArgs)

	for i := range strs {
		if strs[i], err = e.stringArg(c.Args[i+1], t, c.Func); err != nil {
			return nil, err
		}
	}

	dst, repl, src, pattern := strs[0], strs[1], strs[2], strs[3]

	re, rerr := regexp.Compile("^(?:" + pattern + ")$")
	if rerr != nil {
		return nil, evalErr("invalid regular expression in label_replace(): %s", pattern)
	}

	if !validLabelName(dst) {
		return nil, evalErr("invalid destination label name in label_replace(): %s", dst)
	}

	out := make(Vector, len(vec))

	for i, s := range vec {
		out[i] = VectorSample{Metric: replaceLabel(s.Metric, re, dst, repl, src), T: s.T, V: s.V}
	}

	return out, nil
}

func replaceLabel(m Labels, re *regexp.Regexp, dst, repl, src string) Labels {
	val := m[src]

	idx := re.FindStringSubmatchIndex(val)
	if idx == nil {
		return m
	}

	res := string(re.ExpandString(nil, repl, val, idx))
	out := m.clone()

	if res == "" {
		delete(out, dst)
	} else {
		out[dst] = res
	}

	return out
}

func (e *Engine) callLabelJoin(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	strs := make([]string, len(c.Args)-1)

	for i := range strs {
		if strs[i], err = e.stringArg(c.Args[i+1], t, c.Func); err != nil {
			return nil, err
		}
	}

	dst, sep, srcs := strs[0], strs[1], strs[2:]
	if !validLabelName(dst) {
		return nil, evalErr("invalid destination label name in label_join(): %s", dst)
	}

	out := make(Vector, len(vec))

	for i, s := range vec {
		parts := make([]string, len(srcs))
		for j, n := range srcs {
			parts[j] = s.Metric[n]
		}

		m := s.Metric.clone()
		if joined := strings.Join(parts, sep); joined == "" {
			delete(m, dst)
		} else {
			m[dst] = joined
		}

		out[i] = VectorSample{Metric: m, T: s.T, V: s.V}
	}

	return out, nil
}

type bucket struct {
	upper, count float64
}

func (e *Engine) callHistogramQuantile(c *Call, t int64) (Value, error) {
	q, err := e.scalarArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	vec, err := e.instantArg(c.Args[1], t, c.Func)
	if err != nil {
		return nil, err
	}

	groups := map[string]*histGroup{}
	order := []string{}

	for _, s := range vec {
		le, perr := strconv.ParseFloat(s.Metric["le"], 64)
		if perr != nil {
			continue
		}

		lbls := s.Metric.without(NameLabel, "le")
		k := Fingerprint(lbls)

		g, ok := groups[k]
		if !ok {
			g = &histGroup{labels: lbls}
			groups[k] = g
			order = append(order, k)
		}

		g.buckets = append(g.buckets, bucket{upper: le, count: s.V})
	}

	out := Vector{}

	for _, k := range order {
		g := groups[k]
		out = append(out, VectorSample{Metric: g.labels, T: t, V: bucketQuantile(q, g.buckets)})
	}

	sortVector(out)

	return out, nil
}

type histGroup struct {
	labels  Labels
	buckets []bucket
}

func bucketQuantile(q float64, bs []bucket) float64 {
	switch {
	case isNaN(q):
		return math.NaN()
	case q < 0:
		return math.Inf(-1)
	case q > 1:
		return math.Inf(1)
	}

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
	if total == 0 {
		return math.NaN()
	}

	rank := q * total
	b := slices.IndexFunc(bs, func(x bucket) bool { return x.count >= rank })

	return interpolateBucket(bs, b, rank)
}

func interpolateBucket(bs []bucket, b int, rank float64) float64 {
	if b == len(bs)-1 {
		return bs[len(bs)-2].upper
	}

	if b == 0 && bs[0].upper <= 0 {
		return bs[0].upper
	}

	start, prev := 0.0, 0.0
	if b > 0 {
		start, prev = bs[b-1].upper, bs[b-1].count
	}

	end, count := bs[b].upper, bs[b].count-prev
	if count == 0 {
		return end
	}

	return start + (end-start)*((rank-prev)/count)
}

func linearRegression(s []Sample, interceptT int64) (float64, float64) {
	var n, sx, sy, sxy, sxx float64

	for _, p := range s {
		x := float64(p.T-interceptT) / msPerSecond
		n++
		sx += x
		sy += p.V
		sxy += x * p.V
		sxx += x * x
	}

	cov := sxy - sx*sy/n
	variance := sxx - sx*sx/n
	slope := cov / variance

	return slope, sy/n - slope*sx/n
}

func derivFn(s []Sample, _, _ int64) (float64, bool) {
	if len(s) < minSamples {
		return 0, false
	}

	slope, _ := linearRegression(s, s[0].T)

	return slope, true
}

func (e *Engine) callPredictLinear(c *Call, t int64) (Value, error) {
	secs, err := e.scalarArg(c.Args[1], t, c.Func)
	if err != nil {
		return nil, err
	}

	fn := func(s []Sample, _, end int64) (float64, bool) {
		if len(s) < minSamples {
			return 0, false
		}

		slope, icpt := linearRegression(s, end)

		return slope*secs + icpt, true
	}

	return e.rangeOver(c.Args[0], t, c.Func, fn)
}

func (e *Engine) callQuantileOverTime(c *Call, t int64) (Value, error) {
	q, err := e.scalarArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	return e.rangeOver(c.Args[1], t, c.Func, overTime(func(v []float64) float64 { return quantile(q, v) }))
}

func (e *Engine) callTimestamp(c *Call, t int64) (Value, error) {
	vec, err := e.instantArg(c.Args[0], t, c.Func)
	if err != nil {
		return nil, err
	}

	out := make(Vector, len(vec))
	for i, s := range vec {
		out[i] = VectorSample{Metric: s.Metric.without(NameLabel), T: s.T, V: float64(s.T) / msPerSecond}
	}

	return out, nil
}

func (e *Engine) callDate(c *Call, t int64, f func(time.Time) float64) (Value, error) {
	vec := Vector{{Metric: Labels{}, T: t, V: float64(t) / msPerSecond}}

	if len(c.Args) == 1 {
		var err error
		if vec, err = e.instantArg(c.Args[0], t, c.Func); err != nil {
			return nil, err
		}
	}

	out := make(Vector, len(vec))

	for i, s := range vec {
		ts := time.Unix(int64(s.V), 0).UTC()
		out[i] = VectorSample{Metric: s.Metric.without(NameLabel), T: s.T, V: f(ts)}
	}

	return out, nil
}

// window is the sample window (start, end] of a subquery evaluated at t.
func (sq *Subquery) window(t int64) (int64, int64) {
	if sq.At != nil {
		t = *sq.At
	}

	end := t - sq.Offset.Milliseconds()

	return end - sq.Range.Milliseconds(), end
}

func (e *Engine) evalSubquery(sq *Subquery, t int64) (Matrix, error) {
	step := sq.Step
	if step <= 0 {
		step = DefaultSubqueryStep
	}

	stepMs := step.Milliseconds()
	if stepMs <= 0 || sq.Range <= 0 {
		return nil, evalErr("subquery range and step must be positive")
	}

	if sq.Range.Milliseconds()/stepMs > MaxRangeSteps {
		return nil, evalErr("exceeded maximum resolution of %d points per timeseries", MaxRangeSteps)
	}

	start, end := sq.window(t)
	first := floorDiv(start, stepMs) * stepMs

	if first <= start {
		first += stepMs
	}

	acc := map[string]*Series{}
	order := []string{}

	for ts := first; ts <= end; ts += stepMs {
		if e.steps++; e.steps > maxSubquerySteps {
			return nil, evalErr("query exceeds the subquery evaluation budget")
		}

		v, err := e.eval(sq.Expr, ts)
		if err != nil {
			return nil, err
		}

		if cerr := collectPoints(acc, &order, v); cerr != nil {
			return nil, cerr
		}
	}

	out := make(Matrix, 0, len(order))
	for _, k := range order {
		out = append(out, *acc[k])
	}

	sortSeries(out)

	return out, nil
}

func collectPoints(acc map[string]*Series, order *[]string, v Value) error {
	switch x := v.(type) {
	case Scalar:
		addPoint(acc, order, Labels{}, Sample(x))
	case Vector:
		for _, s := range x {
			addPoint(acc, order, s.Metric, Sample{T: s.T, V: s.V})
		}
	default:
		return evalErr("subquery must return an instant vector or scalar, got %s", v.valueType())
	}

	return nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}
