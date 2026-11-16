package ocilogging

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

const (
	maxQueryLen     = 8192
	maxQueryOps     = 32
	maxExprDepth    = 32
	maxStringResult = 65536
	maxSummaryGroup = 10000
	maxIntervalMs   = 365 * 24 * 3600 * 1000
	msPerSecond     = 1000
	msPerMinute     = 60 * msPerSecond
	msPerHour       = 60 * msPerMinute
	msPerDay        = 24 * msPerHour
	callArgs3       = 3
	litTrue         = "true"
)

// vexpr is a scalar expression of the Logging query language.
type vexpr interface {
	value(r *row) (any, bool)
	String() string
}

type litExpr struct{ v any }

type pathExpr struct{ path []string }

type negExpr struct{ e vexpr }

type binExpr struct {
	l, r vexpr
	op   byte
}

type callExpr struct {
	name string
	args []vexpr
}

func (e litExpr) value(*row) (any, bool) { return e.v, true }
func (e litExpr) String() string         { return scalarString(e.v) }

func (e pathExpr) value(r *row) (any, bool) { return r.lookup(e.path) }
func (e pathExpr) String() string           { return strings.Join(e.path, ".") }

func (e negExpr) String() string { return "-" + e.e.String() }

func (e negExpr) value(r *row) (any, bool) {
	f, ok := numVal(e.e, r)

	return -f, ok
}

func (e binExpr) String() string { return e.l.String() + string(e.op) + e.r.String() }

func (e binExpr) value(r *row) (any, bool) {
	l, lok := numVal(e.l, r)
	rv, rok := numVal(e.r, r)

	if !lok || !rok {
		return nil, false
	}

	var out float64

	switch e.op {
	case '+':
		out = l + rv
	case '-':
		out = l - rv
	case '*':
		out = l * rv
	default:
		if rv == 0 {
			return nil, false
		}

		out = l / rv
	}

	return out, !math.IsInf(out, 0)
}

func numVal(e vexpr, r *row) (float64, bool) {
	v, ok := e.value(r)
	if !ok {
		return 0, false
	}

	f, isNum := toFloat(v)

	return f, isNum && !math.IsNaN(f) && !math.IsInf(f, 0)
}

func (e callExpr) String() string {
	parts := make([]string, 0, len(e.args))
	for _, a := range e.args {
		parts = append(parts, a.String())
	}

	return e.name + "(" + strings.Join(parts, ", ") + ")"
}

// scalarFuncs maps a function name to its minimum and maximum argument counts.
var scalarFuncs = map[string][2]int{ //nolint:gochecknoglobals // immutable lookup table
	"not": {1, 1}, "contains_ci": {2, callArgs3}, "contains_cs": {2, callArgs3},
	"rounddown": {2, 2},
	"concat":    {1, maxExprDepth}, "upper": {1, 1}, "lower": {1, 1}, "substr": {2, callArgs3},
	"isnull": {1, 1}, "isnotnull": {1, 1},
}

var intervalRE = regexp.MustCompile(`^([0-9]{1,9})([dhms])$`)

func intervalMs(s string) (float64, bool) {
	m := intervalRE.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}

	n, _ := strconv.Atoi(m[1])
	units := map[string]int{
		"s": msPerSecond, "m": msPerMinute, "h": msPerHour, "d": msPerDay,
	}
	unit := units[m[2]]

	if n == 0 || n*unit > maxIntervalMs {
		return 0, false
	}

	return float64(n * unit), true
}

func (e callExpr) value(r *row) (any, bool) {
	vals := make([]any, len(e.args))
	oks := make([]bool, len(e.args))

	for i, a := range e.args {
		vals[i], oks[i] = a.value(r)
	}

	switch e.name {
	case "not":
		b, _ := vals[0].(bool)

		return !b, true
	case "isnull":
		return !oks[0] || vals[0] == nil, true
	case "isnotnull":
		return oks[0] && vals[0] != nil, true
	case "contains_ci":
		return strings.Contains(strings.ToLower(scalarString(vals[0])), strings.ToLower(scalarString(vals[1]))), true
	case "contains_cs":
		return strings.Contains(scalarString(vals[0]), scalarString(vals[1])), true
	case "upper":
		return strings.ToUpper(scalarString(vals[0])), oks[0]
	case "lower":
		return strings.ToLower(scalarString(vals[0])), oks[0]
	case "concat":
		return e.concat(vals), true
	case "substr":
		return e.substr(vals, oks)
	}

	return e.rounddown(vals, oks)
}

func (e callExpr) concat(vals []any) any {
	var sb strings.Builder

	for _, v := range vals {
		sb.WriteString(scalarString(v))

		if sb.Len() > maxStringResult {
			return sb.String()[:maxStringResult]
		}
	}

	return sb.String()
}

func (e callExpr) substr(vals []any, oks []bool) (any, bool) {
	start, sok := toFloat(vals[1])
	if !oks[0] || !sok || start < 0 {
		return nil, false
	}

	rs := []rune(scalarString(vals[0]))
	from := min(int(min(start, float64(len(rs)))), len(rs))
	to := len(rs)

	if len(vals) == callArgs3 {
		n, nok := toFloat(vals[2])
		if !nok || n < 0 {
			return nil, false
		}

		to = min(from+int(min(n, float64(len(rs)))), len(rs))
	}

	return string(rs[from:to]), true
}

func (e callExpr) rounddown(vals []any, oks []bool) (any, bool) {
	ms, mok := toFloat(vals[0])
	iv, iok := intervalMs(scalarString(vals[1]))

	if !oks[0] || !mok || !iok {
		return nil, false
	}

	return math.Floor(ms/iv) * iv, true
}

// Parsing.

func (p *parser) enter() *APIError {
	p.depth++
	if p.depth > maxExprDepth {
		return errParam("the expression is nested too deeply")
	}

	return nil
}

func (p *parser) parseAdd() (vexpr, *APIError) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer func() { p.depth-- }()

	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()

		var op byte

		switch {
		case t.kind == tkPunct && (t.text == "+" || t.text == "-"):
			op = t.text[0]

			p.next()
		case t.kind == tkNumber && strings.HasPrefix(t.text, "-"):
			f, perr := strconv.ParseFloat(t.text[1:], 64)
			if perr != nil {
				return nil, errParam("invalid number " + t.text)
			}

			p.next()

			l = binExpr{l: l, r: litExpr{f}, op: '-'}

			continue
		default:
			return l, nil
		}

		r, rerr := p.parseMul()
		if rerr != nil {
			return nil, rerr
		}

		l = binExpr{l: l, r: r, op: op}
	}
}

func (p *parser) parseMul() (vexpr, *APIError) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.isPunct("*") || p.isPunct("/") {
		op := p.next().text[0]

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = binExpr{l: l, r: r, op: op}
	}

	return l, nil
}

func (p *parser) parseUnary() (vexpr, *APIError) {
	if p.isPunct("-") {
		p.next()

		e, err := p.parseUnary()

		return negExpr{e}, err
	}

	return p.parsePrimary()
}

func (p *parser) parsePrimary() (vexpr, *APIError) {
	t := p.next()

	switch {
	case t.kind == tkString:
		return litExpr{t.text}, nil
	case t.kind == tkNumber:
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, errParam("invalid number " + t.text)
		}

		return litExpr{f}, nil
	case t.kind == tkPunct && t.text == "(":
		e, err := p.parseAdd()
		if err != nil {
			return nil, err
		}

		if !p.isPunct(")") {
			return nil, errParam("missing ) in search query")
		}

		p.next()

		return e, nil
	case t.kind == tkPath:
		return p.parseWord(t)
	}

	return nil, errParam("expected an expression in search query, got " + strconv.Quote(t.text))
}

// parseWord reads a function call, a true, false or null literal, or a field path.
func (p *parser) parseWord(t token) (vexpr, *APIError) {
	if len(t.path) != 1 || t.quoted {
		return pathExpr{t.path}, nil
	}

	word := strings.ToLower(t.path[0])
	if p.isPunct("(") {
		return p.parseCall(word)
	}

	switch word {
	case litTrue:
		return litExpr{true}, nil
	case "false":
		return litExpr{false}, nil
	case "null":
		return litExpr{nil}, nil
	}

	return pathExpr{t.path}, nil
}

func (p *parser) parseCall(name string) (vexpr, *APIError) {
	arity, ok := scalarFuncs[name]
	if !ok {
		return nil, errParam("the function " + name + " is not supported by this emulator")
	}

	p.next()

	var args []vexpr

	for !p.isPunct(")") {
		if len(args) > 0 {
			if !p.isPunct(",") {
				return nil, errParam("expected , or ) in the arguments of " + name)
			}

			p.next()
		}

		a, err := p.parseAdd()
		if err != nil {
			return nil, err
		}

		args = append(args, a)
	}

	p.next()

	if len(args) < arity[0] || len(args) > arity[1] {
		return nil, errParam("wrong number of arguments for " + name)
	}

	return args2(name, args)
}

func args2(name string, args []vexpr) (vexpr, *APIError) {
	if name == "rounddown" {
		l, isLit := args[1].(litExpr)
		if _, valid := intervalMs(scalarString(l.v)); !isLit || !valid {
			return nil, errParam("rounddown needs an interval such as '1m', '2h', '30s' or '1d'")
		}
	}

	if strings.HasPrefix(name, "contains_") && len(args) == callArgs3 {
		if l, isLit := args[2].(litExpr); !isLit || !isBool(l.v) {
			return nil, errParam(name + " needs true or false as its third argument")
		}
	}

	return callExpr{name: name, args: args}, nil
}

func isBool(v any) bool {
	_, ok := v.(bool)

	return ok
}
