package timestreamquery

import (
	"bytes"
	"database/sql/driver"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// seriesMark prefixes the JSON text of a timeseries value; arrays carry arrayMark (athena's container mark).
const (
	seriesMark = "\x02"
	arrayMark  = "\x01"
	maxSeqLen  = 1_000_000
	splineDiv  = 6
	splineRHS  = 6
	splineDiag = 2
	midpoint   = 2
)

var (
	errNotSeries   = errors.New("argument is not a timeseries")
	errSeriesAlign = errors.New("the timeseries must have the same timestamps")
	errSeriesType  = errors.New("timeseries values must be numeric for this function")
	errBadSeq      = errors.New("invalid sequence bounds")
)

// seriesPoint is one (time, value) sample; value is a float64, int64, bool or string.
type seriesPoint struct {
	t time.Time
	v any
}

func isSeriesText(v driver.Value) bool {
	s, ok := v.(string)

	return ok && strings.HasPrefix(s, seriesMark)
}

func textOf(v driver.Value) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

func decodeSeries(v driver.Value) ([]seriesPoint, error) {
	s, ok := textOf(v)
	if !ok || !strings.HasPrefix(s, seriesMark) {
		return nil, errNotSeries
	}

	dec := jsontext.NewDecoder(strings.NewReader(strings.TrimPrefix(s, seriesMark)))

	var raw [][]any
	if err := json.UnmarshalDecode(dec, &raw, jsonx.V1(), jsonx.UseNumber()); err != nil {
		return nil, errNotSeries
	}

	pts := make([]seriesPoint, 0, len(raw))

	for _, p := range raw {
		if len(p) != argsTwo {
			return nil, errNotSeries
		}

		ts, _ := p[0].(string)

		t, valid := parseTSValue(ts)
		if !valid {
			return nil, errNotSeries
		}

		pts = append(pts, seriesPoint{t: t, v: decodeJSONScalar(p[1])})
	}

	return pts, nil
}

func decodeJSONScalar(v any) any {
	n, ok := v.(jsonx.Number)
	if !ok {
		return v
	}

	if i, err := n.Int64(); err == nil && !strings.ContainsAny(n.String(), ".eE") {
		return i
	}

	f, _ := n.Float64()

	return f
}

func encodeSeries(pts []seriesPoint) string {
	var b bytes.Buffer

	b.WriteString(seriesMark + "[")

	for i, p := range pts {
		if i > 0 {
			b.WriteByte(',')
		}

		b.WriteString(`["` + formatTS(p.t) + `",` + encodeJSONScalar(p.v) + "]")
	}

	b.WriteByte(']')

	return b.String()
}

func encodeJSONScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return kwNull
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "null"
		}

		s := strconv.FormatFloat(x, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eEn") {
			s += ".0"
		}

		return s
	case bool:
		return strconv.FormatBool(x)
	default:
		b, _ := json.Marshal(fmt.Sprint(x), jsonx.V1())

		return string(b)
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

// seriesAgg is create_time_series(time, value).
type seriesAgg struct{ pts []seriesPoint }

func (a *seriesAgg) Step(_ *fnCtx, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	t, ok := parseTSValue(args[0])
	if !ok {
		return errBadArg
	}

	if args[1] == nil {
		return fmt.Errorf("%w: the second parameter of create_time_series cannot be null", errBadArg)
	}

	v := args[1]
	if b, isBytes := v.([]byte); isBytes {
		v = string(b)
	}

	a.pts = append(a.pts, seriesPoint{t: t, v: v})

	return nil
}

func (*seriesAgg) WindowInverse(_ *fnCtx, _ []driver.Value) error { return errNoWindow }

func (a *seriesAgg) WindowValue(_ *fnCtx) (driver.Value, error) {
	sort.SliceStable(a.pts, func(i, j int) bool { return a.pts[i].t.Before(a.pts[j].t) })

	return encodeSeries(dedupePoints(a.pts)), nil
}

func (*seriesAgg) Final(_ *fnCtx) {}

var errNoWindow = errors.New("create_time_series cannot be used as a window function")

func dedupePoints(pts []seriesPoint) []seriesPoint {
	out := pts[:0:0]

	for _, p := range pts {
		if n := len(out); n > 0 && out[n-1].t.Equal(p.t) {
			out[n-1] = p

			continue
		}

		out = append(out, p)
	}

	return out
}

func numericPoints(v driver.Value) ([]seriesPoint, []float64, error) {
	pts, err := decodeSeries(v)
	if err != nil {
		return nil, nil, err
	}

	ys := make([]float64, len(pts))

	for i, p := range pts {
		f, ok := toFloat(p.v)
		if !ok {
			return nil, nil, errSeriesType
		}

		ys[i] = f
	}

	return pts, ys, nil
}

func targetTimes(v driver.Value) ([]time.Time, bool, error) {
	if t, ok := parseTSValue(v); ok {
		if s, isStr := textOf(v); isStr && !strings.HasPrefix(s, arrayMark) {
			return []time.Time{t}, true, nil
		}
	}

	s, ok := textOf(v)
	if !ok {
		return nil, false, errBadArg
	}

	var raw []string
	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, arrayMark)), &raw, jsonx.V1()); err != nil {
		return nil, false, errBadArg
	}

	out := make([]time.Time, 0, len(raw))

	for _, r := range raw {
		t, ok2 := parseTSValue(r)
		if !ok2 {
			return nil, false, errBadArg
		}

		out = append(out, t)
	}

	return out, false, nil
}

// interpolator computes the value at t, reporting false when the series cannot answer there.
type interpolator func(pts []seriesPoint, ys []float64, t time.Time) (float64, bool)

func floorIndex(pts []seriesPoint, t time.Time) int {
	return sort.Search(len(pts), func(i int) bool { return pts[i].t.After(t) }) - 1
}

func linearAt(pts []seriesPoint, ys []float64, t time.Time) (float64, bool) {
	i := floorIndex(pts, t)
	if i < 0 {
		return 0, false
	}

	if pts[i].t.Equal(t) {
		return ys[i], true
	}

	if i+1 >= len(pts) {
		return 0, false
	}

	span := float64(pts[i+1].t.Sub(pts[i].t))
	frac := float64(t.Sub(pts[i].t)) / span

	return ys[i] + (ys[i+1]-ys[i])*frac, true
}

func locfAt(pts []seriesPoint, ys []float64, t time.Time) (float64, bool) {
	i := floorIndex(pts, t)
	if i < 0 {
		return 0, false
	}

	return ys[i], true
}

// splineAt evaluates a natural cubic spline through the points.
func splineAt(pts []seriesPoint, ys []float64, t time.Time) (float64, bool) {
	n := len(pts)
	if n < argsThree {
		return linearAt(pts, ys, t)
	}

	if t.Before(pts[0].t) || t.After(pts[n-1].t) {
		return 0, false
	}

	xs := make([]float64, n)
	for i, p := range pts {
		xs[i] = float64(p.t.Sub(pts[0].t))
	}

	m := naturalSplineMoments(xs, ys)
	i := min(max(floorIndex(pts, t), 0), n-argsTwo)
	x := float64(t.Sub(pts[0].t))
	h := xs[i+1] - xs[i]
	a, b := (xs[i+1]-x)/h, (x-xs[i])/h

	return a*ys[i] + b*ys[i+1] + ((a*a*a-a)*m[i]+(b*b*b-b)*m[i+1])*h*h/splineDiv, true
}

func naturalSplineMoments(xs, ys []float64) []float64 {
	n := len(xs)
	m := make([]float64, n)
	diag := make([]float64, n)
	rhs := make([]float64, n)
	up := make([]float64, n)

	for i := 1; i < n-1; i++ {
		h0, h1 := xs[i]-xs[i-1], xs[i+1]-xs[i]
		diag[i] = splineDiag * (h0 + h1)
		up[i] = h1
		rhs[i] = splineRHS * ((ys[i+1]-ys[i])/h1 - (ys[i]-ys[i-1])/h0)
	}

	for i := 2; i < n-1; i++ {
		w := (xs[i] - xs[i-1]) / diag[i-1]
		diag[i] -= w * up[i-1]
		rhs[i] -= w * rhs[i-1]
	}

	for i := n - argsTwo; i >= 1; i-- {
		next := 0.0
		if i+1 < n-1 {
			next = m[i+1]
		}

		m[i] = (rhs[i] - up[i]*next) / diag[i]
	}

	return m
}

func udfInterpolate(at interpolator) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		pts, ys, err := numericPoints(args[0])
		if err != nil {
			return nil, err
		}

		times, scalar, err := targetTimes(args[1])
		if err != nil {
			return nil, err
		}

		if scalar {
			if v, ok := at(pts, ys, times[0]); ok {
				return v, nil
			}

			return nil, nil
		}

		out := make([]seriesPoint, 0, len(times))

		for _, t := range times {
			if v, ok := at(pts, ys, t); ok {
				out = append(out, seriesPoint{t: t, v: v})
			}
		}

		return encodeSeries(out), nil
	}
}

func udfInterpolateFill(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil || args[argsTwo] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	pts, ys, err := numericPoints(args[0])
	if err != nil {
		return nil, err
	}

	fill, ok := intOrFloat(args[argsTwo])
	if !ok {
		return nil, errBadArg
	}

	times, scalar, err := targetTimes(args[1])
	if err != nil {
		return nil, err
	}

	at := func(t time.Time) float64 {
		if i := floorIndex(pts, t); i >= 0 && pts[i].t.Equal(t) {
			return ys[i]
		}

		return fill
	}

	if scalar {
		return at(times[0]), nil
	}

	out := make([]seriesPoint, 0, len(times))
	for _, t := range times {
		out = append(out, seriesPoint{t: t, v: at(t)})
	}

	return encodeSeries(out), nil
}

func intOrFloat(v driver.Value) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func udfDerivative(nonNegative bool) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		pts, ys, err := numericPoints(args[0])
		if err != nil {
			return nil, err
		}

		unit, ok := intOrFloat(args[1])
		if !ok || unit <= 0 {
			return nil, errBadArg
		}

		out := make([]seriesPoint, 0, len(pts))

		for i := 1; i < len(pts); i++ {
			d := (ys[i] - ys[i-1]) / (float64(pts[i].t.Sub(pts[i-1].t)) / unit)
			if nonNegative && d < 0 {
				continue
			}

			out = append(out, seriesPoint{t: pts[i].t, v: d})
		}

		return encodeSeries(out), nil
	}
}

func udfIntegral(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	unit := float64(time.Second)

	if len(args) > 1 && args[1] != nil {
		u, ok := intOrFloat(args[1])
		if !ok || u <= 0 {
			return nil, errBadArg
		}

		unit = u
	}

	pts, ys, err := numericPoints(args[0])
	if err != nil {
		return nil, err
	}

	var area float64

	for i := 1; i < len(pts); i++ {
		area += (ys[i] + ys[i-1]) / midpoint * (float64(pts[i].t.Sub(pts[i-1].t)) / unit)
	}

	return area, nil
}

func alignedSeries(a, b driver.Value) ([]float64, []float64, error) {
	pa, xa, err := numericPoints(a)
	if err != nil {
		return nil, nil, err
	}

	pb, xb, err := numericPoints(b)
	if err != nil {
		return nil, nil, err
	}

	if len(pa) != len(pb) {
		return nil, nil, errSeriesAlign
	}

	for i := range pa {
		if !pa[i].t.Equal(pb[i].t) {
			return nil, nil, errSeriesAlign
		}
	}

	return xa, xb, nil
}

func pearson(x, y []float64) (float64, bool) {
	n := float64(len(x))
	if len(x) < argsTwo {
		return 0, false
	}

	var sx, sy float64

	for i := range x {
		sx += x[i]
		sy += y[i]
	}

	mx, my := sx/n, sy/n

	var cov, vx, vy float64

	for i := range x {
		cov += (x[i] - mx) * (y[i] - my)
		vx += (x[i] - mx) * (x[i] - mx)
		vy += (y[i] - my) * (y[i] - my)
	}

	if vx == 0 || vy == 0 {
		return 0, false
	}

	return cov / math.Sqrt(vx*vy), true
}

func ranks(v []float64) []float64 {
	idx := make([]int, len(v))
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool { return v[idx[a]] < v[idx[b]] })

	out := make([]float64, len(v))

	for i := 0; i < len(idx); {
		j := i
		for j+1 < len(idx) && v[idx[j+1]] == v[idx[i]] {
			j++
		}

		avg := float64(i+j)/midpoint + 1
		for k := i; k <= j; k++ {
			out[idx[k]] = avg
		}

		i = j + 1
	}

	return out
}

func udfCorrelate(spearman bool) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		x, y, err := alignedSeries(args[0], args[1])
		if err != nil {
			return nil, err
		}

		if spearman {
			x, y = ranks(x), ranks(y)
		}

		if r, ok := pearson(x, y); ok {
			return r, nil
		}

		return math.NaN(), nil
	}
}

// udfSequence is sequence(start, stop, step): timestamps step in nanoseconds, integers step as given.
func udfSequence(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	for _, a := range args {
		if a == nil {
			return nil, nil //nolint:nilnil // SQL NULL
		}
	}

	if start, ok := parseTSValue(args[0]); ok {
		return sequenceOfTimes(start, args)
	}

	return sequenceOfInts(args)
}

func sequenceOfTimes(start time.Time, args []driver.Value) (driver.Value, error) {
	stop, ok := parseTSValue(args[1])
	if !ok || len(args) <= argsTwo {
		return nil, errBadSeq
	}

	step, ok := intArg(args[argsTwo])
	if !ok || step == 0 {
		return nil, errBadSeq
	}

	return sequenceTimes(start, stop, step)
}

func sequenceOfInts(args []driver.Value) (driver.Value, error) {
	lo, ok1 := intArg(args[0])
	hi, ok2 := intArg(args[1])
	step := int64(1)

	if len(args) > argsTwo {
		var ok3 bool
		if step, ok3 = intArg(args[argsTwo]); !ok3 {
			return nil, errBadSeq
		}
	}

	if !ok1 || !ok2 || step == 0 || (hi-lo)/step >= maxSeqLen {
		return nil, errBadSeq
	}

	var vals []int64
	for v := lo; (step > 0 && v <= hi) || (step < 0 && v >= hi); v += step {
		vals = append(vals, v)
	}

	b, _ := json.Marshal(vals, jsonx.V1())

	return arrayMark + string(b), nil
}

func sequenceTimes(start, stop time.Time, stepNs int64) (driver.Value, error) {
	step := time.Duration(stepNs)
	if int64(stop.Sub(start)/step) >= maxSeqLen {
		return nil, errBadSeq
	}

	var vals []string

	for t := start; (step > 0 && !t.After(stop)) || (step < 0 && !t.Before(stop)); t = t.Add(step) {
		vals = append(vals, formatTS(t))
	}

	b, _ := json.Marshal(vals, jsonx.V1())

	return arrayMark + string(b), nil
}

func udfSeriesJSON(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := textOf(args[0])
	if !ok {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	return strings.TrimPrefix(s, seriesMark), nil
}
