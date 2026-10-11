package cloudwatch

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var errMetricExpr = errors.New("invalid metric math expression")

type mathTokKind int

const (
	tokNum mathTokKind = iota
	tokIdent
	tokOp
	tokEOF
)

type mathTok struct {
	text string
	num  float64
	kind mathTokKind
}

// mathVal is either a scalar or a single time series.
type mathVal struct {
	series   metricTimeSeries
	scalar   float64
	isSeries bool
}

func isMathIdentStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isMathIdentPart(ch byte) bool {
	return isMathIdentStart(ch) || ch >= '0' && ch <= '9'
}

func scanMathOp(expr string, i int) (string, error) {
	if i+1 < len(expr) {
		switch two := expr[i : i+2]; two {
		case ">=", "<=", "==", "!=", "&&", "||":
			return two, nil
		}
	}

	if !strings.ContainsRune("+-*/^(),<>!", rune(expr[i])) {
		return "", fmt.Errorf("%w: unexpected %q", errMetricExpr, expr[i])
	}

	return expr[i : i+1], nil
}

func tokenizeMath(expr string) ([]mathTok, error) {
	var toks []mathTok

	for i := 0; i < len(expr); {
		ch := expr[i]

		switch {
		case ch == ' ' || ch == '\t' || ch == '\n':
			i++
		case ch >= '0' && ch <= '9' || ch == '.':
			j := scanMathNumber(expr, i)

			f, err := strconv.ParseFloat(expr[i:j], 64)
			if err != nil {
				return nil, fmt.Errorf("%w: bad number %q", errMetricExpr, expr[i:j])
			}

			toks = append(toks, mathTok{kind: tokNum, num: f})
			i = j
		case isMathIdentStart(ch):
			j := i
			for j < len(expr) && isMathIdentPart(expr[j]) {
				j++
			}

			toks = append(toks, mathTok{kind: tokIdent, text: expr[i:j]})
			i = j
		default:
			op, err := scanMathOp(expr, i)
			if err != nil {
				return nil, err
			}

			toks = append(toks, mathTok{kind: tokOp, text: op})
			i += len(op)
		}
	}

	return append(toks, mathTok{kind: tokEOF}), nil
}

type mathParser struct {
	resolved map[string]MetricDataResult
	toks     []mathTok
	pos      int
}

const (
	precOr = iota + 1
	precAnd
	precCompare
	precAdd
	precMul
	precPow
)

func mathBinPrec(op string) (int, bool) {
	switch op {
	case "||":
		return precOr, true
	case "&&":
		return precAnd, true
	case "==", "!=", "<", ">", "<=", ">=":
		return precCompare, true
	case "+", "-":
		return precAdd, true
	case "*", "/":
		return precMul, true
	case "^":
		return precPow, true
	}

	return 0, false
}

func (p *mathParser) peek() mathTok { return p.toks[p.pos] }

func (p *mathParser) next() mathTok {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *mathParser) expectOp(op string) error {
	if t := p.next(); t.kind != tokOp || t.text != op {
		return fmt.Errorf("%w: expected %q", errMetricExpr, op)
	}

	return nil
}

func (p *mathParser) parseExpr(minPrec int) (mathVal, error) {
	left, err := p.parseUnary()
	if err != nil {
		return mathVal{}, err
	}

	for {
		t := p.peek()
		prec, isBin := mathBinPrec(t.text)

		if t.kind != tokOp || !isBin || prec < minPrec {
			return left, nil
		}

		p.next()

		nextMin := prec + 1
		if t.text == "^" {
			nextMin = prec
		}

		right, rerr := p.parseExpr(nextMin)
		if rerr != nil {
			return mathVal{}, rerr
		}

		left = mathBinary(t.text, left, right)
	}
}

func (p *mathParser) parseUnary() (mathVal, error) {
	t := p.peek()
	if t.kind == tokOp && (t.text == "-" || t.text == "+" || t.text == "!") {
		p.next()

		v, err := p.parseUnary()
		if err != nil {
			return mathVal{}, err
		}

		return mathMap(v, func(x float64) float64 {
			switch t.text {
			case "-":
				return -x
			case "!":
				return boolFloat(x == 0)
			}

			return x
		}), nil
	}

	return p.parsePrimary()
}

func (p *mathParser) parsePrimary() (mathVal, error) {
	t := p.next()

	switch {
	case t.kind == tokNum:
		return mathVal{scalar: t.num}, nil
	case t.kind == tokOp && t.text == "(":
		v, err := p.parseExpr(1)
		if err != nil {
			return mathVal{}, err
		}

		return v, p.expectOp(")")
	case t.kind == tokIdent:
		if nt := p.peek(); nt.kind == tokOp && nt.text == "(" {
			p.next()

			return p.parseCall(strings.ToUpper(t.text))
		}

		r, ok := p.resolved[t.text]
		if !ok {
			return mathVal{}, fmt.Errorf("%w: unknown metric id %q", errMetricExpr, t.text)
		}

		return mathVal{isSeries: true, series: buildTimeSeries(r)}, nil
	}

	return mathVal{}, fmt.Errorf("%w: unexpected token", errMetricExpr)
}

func (p *mathParser) parseCall(fn string) (mathVal, error) {
	var args []mathVal

	if nt := p.peek(); nt.kind == tokOp && nt.text == ")" {
		p.next()
	} else {
		for {
			a, err := p.parseExpr(1)
			if err != nil {
				return mathVal{}, err
			}

			args = append(args, a)

			t := p.next()
			if t.kind == tokOp && t.text == ")" {
				break
			}

			if t.kind != tokOp || t.text != "," {
				return mathVal{}, fmt.Errorf("%w: bad argument list", errMetricExpr)
			}
		}
	}

	return callMathFn(fn, args)
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

func mathMap(v mathVal, f func(float64) float64) mathVal {
	if !v.isSeries {
		return mathVal{scalar: f(v.scalar)}
	}

	out := make(metricTimeSeries, len(v.series))
	for k, x := range v.series {
		out[k] = f(x)
	}

	return mathVal{isSeries: true, series: out}
}

func mathOp(op string, a, b float64) float64 {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		if b == 0 {
			return math.NaN()
		}

		return a / b
	case "^":
		return math.Pow(a, b)
	}

	return mathCompareOp(op, a, b)
}

func mathCompareOp(op string, a, b float64) float64 {
	switch op {
	case "==":
		return boolFloat(a == b)
	case "!=":
		return boolFloat(a != b)
	case "<":
		return boolFloat(a < b)
	case ">":
		return boolFloat(a > b)
	case "<=":
		return boolFloat(a <= b)
	case ">=":
		return boolFloat(a >= b)
	case "&&":
		return boolFloat(a != 0 && b != 0)
	case "||":
		return boolFloat(a != 0 || b != 0)
	}

	return math.NaN()
}

func mathBinary(op string, l, r mathVal) mathVal {
	switch {
	case !l.isSeries && !r.isSeries:
		return mathVal{scalar: mathOp(op, l.scalar, r.scalar)}
	case l.isSeries && !r.isSeries:
		return mathMap(l, func(x float64) float64 { return mathOp(op, x, r.scalar) })
	case !l.isSeries && r.isSeries:
		return mathMap(r, func(x float64) float64 { return mathOp(op, l.scalar, x) })
	}

	out := make(metricTimeSeries)

	for k, lv := range l.series {
		if rv, ok := r.series[k]; ok {
			out[k] = mathOp(op, lv, rv)
		}
	}

	return mathVal{isSeries: true, series: out}
}

func seriesVals(v mathVal) ([]int64, []float64) {
	keys := mergedKeys(v.series)
	vals := make([]float64, len(keys))

	for i, k := range keys {
		vals[i] = v.series[k]
	}

	return keys, vals
}

func callMathFn(fn string, args []mathVal) (mathVal, error) {
	unary := map[string]func(float64) float64{
		"ABS": math.Abs, "CEIL": math.Ceil, "FLOOR": math.Floor,
		"LOG": math.Log, "LOG10": math.Log10,
	}

	if f, ok := unary[fn]; ok {
		if len(args) != 1 {
			return mathVal{}, fmt.Errorf("%w: %s takes one argument", errMetricExpr, fn)
		}

		return mathMap(args[0], f), nil
	}

	aggs := map[string]func([]float64) float64{
		fnSum: aggSum, fnAvg: aggAvg, fnMin: aggMin, fnMax: aggMax, fnStddev: aggStddev,
	}

	if f, ok := aggs[fn]; ok {
		if len(args) != 1 || !args[0].isSeries {
			return mathVal{}, fmt.Errorf("%w: %s needs a time series", errMetricExpr, fn)
		}

		_, vals := seriesVals(args[0])

		return mathVal{scalar: f(vals)}, nil
	}

	return callMathSeriesFn(fn, args)
}

func callMathSeriesFn(fn string, args []mathVal) (mathVal, error) {
	switch fn {
	case "IF":
		return mathIf(args)
	case "RUNNING_SUM", "DIFF", "DIFF_TIME", "DATAPOINT_COUNT":
		if len(args) != 1 || !args[0].isSeries {
			return mathVal{}, fmt.Errorf("%w: %s needs a time series", errMetricExpr, fn)
		}

		return mathSeriesTransform(fn, args[0]), nil
	}

	return mathVal{}, fmt.Errorf("%w: unsupported function %s", errMetricExpr, fn)
}

func mathIf(args []mathVal) (mathVal, error) {
	const ifArgs = 3
	if len(args) != ifArgs {
		return mathVal{}, fmt.Errorf("%w: IF takes three arguments", errMetricExpr)
	}

	cond, a, b := args[0], args[1], args[2]
	if !cond.isSeries {
		if cond.scalar != 0 {
			return a, nil
		}

		return b, nil
	}

	out := make(metricTimeSeries, len(cond.series))
	pick := func(v mathVal, k int64) (float64, bool) {
		if !v.isSeries {
			return v.scalar, true
		}

		x, ok := v.series[k]

		return x, ok
	}

	for k, c := range cond.series {
		src := b
		if c != 0 {
			src = a
		}

		if x, ok := pick(src, k); ok {
			out[k] = x
		}
	}

	return mathVal{isSeries: true, series: out}, nil
}

func mathSeriesTransform(fn string, v mathVal) mathVal {
	keys, vals := seriesVals(v)
	out := make(metricTimeSeries, len(keys))

	var running float64

	for i, k := range keys {
		switch fn {
		case "RUNNING_SUM":
			running += vals[i]
			out[k] = running
		case "DATAPOINT_COUNT":
			out[k] = 1
		case "DIFF":
			if i > 0 {
				out[k] = vals[i] - vals[i-1]
			}
		case "DIFF_TIME":
			if i > 0 {
				out[k] = time.Unix(k, 0).Sub(time.Unix(keys[i-1], 0)).Seconds()
			}
		}
	}

	return mathVal{isSeries: true, series: out}
}

// evalGeneralExpr evaluates compound metric math (nesting, precedence,
// comparison, IF, RUNNING_SUM, DIFF, ABS...). ok is false on a parse or
// evaluation error.
func evalGeneralExpr(expr string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	toks, err := tokenizeMath(expr)
	if err != nil {
		return false
	}

	p := &mathParser{toks: toks, resolved: resolved}

	v, perr := p.parseExpr(1)
	if perr != nil || p.peek().kind != tokEOF || !v.isSeries {
		return false
	}

	result.Timestamps, result.Values = timeSeriesResult(v.series)

	return true
}

func scanMathNumber(expr string, i int) int {
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }

	j := i
	for j < len(expr) && (isDigit(expr[j]) || expr[j] == '.') {
		j++
	}

	if j < len(expr) && (expr[j] == 'e' || expr[j] == 'E') {
		k := j + 1
		if k < len(expr) && (expr[k] == '+' || expr[k] == '-') {
			k++
		}

		if k < len(expr) && isDigit(expr[k]) {
			for k < len(expr) && isDigit(expr[k]) {
				k++
			}

			j = k
		}
	}

	return j
}
