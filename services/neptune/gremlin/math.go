package gremlin

import (
	"math"
	"slices"
	"strconv"
)

func regMath(m map[string]stepFn) {
	m["math"] = stepMath
}

type mathExpr interface {
	eval(vars map[string]float64) float64
}

type (
	mathNum float64
	mathVar string
	mathNeg struct{ x mathExpr }
	mathBin struct {
		l, r mathExpr
		op   byte
	}
	mathCall struct {
		fn func(float64) float64
		x  mathExpr
	}
)

func (n mathNum) eval(map[string]float64) float64 { return float64(n) }

func (v mathVar) eval(vars map[string]float64) float64 { return vars[string(v)] }

func (n mathNeg) eval(vars map[string]float64) float64 { return -n.x.eval(vars) }

func (c mathCall) eval(vars map[string]float64) float64 { return c.fn(c.x.eval(vars)) }

func (b mathBin) eval(vars map[string]float64) float64 {
	l, r := b.l.eval(vars), b.r.eval(vars)

	switch b.op {
	case '+':
		return l + r
	case '-':
		return l - r
	case '*':
		return l * r
	case '/':
		return l / r
	case '%':
		return math.Mod(l, r)
	}

	return math.Pow(l, r)
}

var mathFuncs = map[string]func(float64) float64{ //nolint:gochecknoglobals // immutable function table
	"abs": math.Abs, "acos": math.Acos, "asin": math.Asin, "atan": math.Atan, "cbrt": math.Cbrt,
	"ceil": math.Ceil, "cos": math.Cos, "cosh": math.Cosh, "exp": math.Exp, "floor": math.Floor,
	"log": math.Log, "log10": math.Log10, "log2": math.Log2, "sin": math.Sin, "sinh": math.Sinh,
	"sqrt": math.Sqrt, "tan": math.Tan, "tanh": math.Tanh, "signum": signum,
}

func signum(f float64) float64 {
	switch {
	case f > 0:
		return 1
	case f < 0:
		return -1
	}

	return f
}

type mathParser struct {
	src  string
	vars []string
	pos  int
}

// parseMath compiles an expression and returns its variables in order of first appearance.
func parseMath(src string) (mathExpr, []string, error) {
	p := &mathParser{src: src}

	e, err := p.expr()
	if err != nil {
		return nil, nil, err
	}

	if p.skip(); p.pos < len(p.src) {
		return nil, nil, p.fail()
	}

	return e, p.vars, nil
}

func (p *mathParser) fail() error {
	return malformed("Unparseable math() expression %q at offset %d", p.src, p.pos)
}

func (p *mathParser) skip() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *mathParser) peek() byte {
	p.skip()

	if p.pos < len(p.src) {
		return p.src[p.pos]
	}

	return 0
}

func (p *mathParser) expr() (mathExpr, error) {
	l, err := p.term()
	if err != nil {
		return nil, err
	}

	for c := p.peek(); c == '+' || c == '-'; c = p.peek() {
		p.pos++

		r, terr := p.term()
		if terr != nil {
			return nil, terr
		}

		l = mathBin{op: c, l: l, r: r}
	}

	return l, nil
}

func (p *mathParser) term() (mathExpr, error) {
	l, err := p.unary()
	if err != nil {
		return nil, err
	}

	for c := p.peek(); c == '*' || c == '/' || c == '%'; c = p.peek() {
		p.pos++

		r, uerr := p.unary()
		if uerr != nil {
			return nil, uerr
		}

		l = mathBin{op: c, l: l, r: r}
	}

	return l, nil
}

func (p *mathParser) unary() (mathExpr, error) {
	switch p.peek() {
	case '-':
		p.pos++

		x, err := p.unary()

		return mathNeg{x: x}, err
	case '+':
		p.pos++

		return p.unary()
	}

	return p.power()
}

func (p *mathParser) power() (mathExpr, error) {
	l, err := p.atom()
	if err != nil {
		return nil, err
	}

	if p.peek() != '^' {
		return l, nil
	}

	p.pos++

	r, err := p.unary()
	if err != nil {
		return nil, err
	}

	return mathBin{op: '^', l: l, r: r}, nil
}

func (p *mathParser) atom() (mathExpr, error) {
	c := p.peek()

	switch {
	case c == '(':
		p.pos++

		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		if p.peek() != ')' {
			return nil, p.fail()
		}

		p.pos++

		return e, nil
	case c >= '0' && c <= '9' || c == '.':
		return p.number()
	case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		return p.ident()
	}

	return nil, p.fail()
}

func (p *mathParser) number() (mathExpr, error) {
	start := p.pos

	for p.pos < len(p.src) && (p.src[p.pos] >= '0' && p.src[p.pos] <= '9' || p.src[p.pos] == '.') {
		p.pos++
	}

	if p.pos < len(p.src) && (p.src[p.pos] == 'e' || p.src[p.pos] == 'E') {
		p.pos++

		if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
			p.pos++
		}

		for p.pos < len(p.src) && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
			p.pos++
		}
	}

	f, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		p.pos = start

		return nil, p.fail()
	}

	return mathNum(f), nil
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func (p *mathParser) ident() (mathExpr, error) {
	start := p.pos
	for p.pos < len(p.src) && isIdentByte(p.src[p.pos]) {
		p.pos++
	}

	name := p.src[start:p.pos]

	if p.peek() == '(' {
		fn, ok := mathFuncs[name]
		if !ok {
			p.pos = start

			return nil, malformed("Unknown function %q in math() expression %q", name, p.src)
		}

		p.pos++

		x, err := p.expr()
		if err != nil {
			return nil, err
		}

		if p.peek() != ')' {
			return nil, p.fail()
		}

		p.pos++

		return mathCall{fn: fn, x: x}, nil
	}

	if !slices.Contains(p.vars, name) {
		p.vars = append(p.vars, name)
	}

	return mathVar(name), nil
}

func stepMath(x *exec, s *Step, in []*trav) ([]*trav, error) {
	if len(s.Args) != 1 {
		return nil, invalidArg("math() takes one expression")
	}

	src, ok := s.Args[0].(string)
	if !ok {
		return nil, invalidArg("math() expects a string expression")
	}

	expr, names, err := parseMath(src)
	if err != nil {
		return nil, err
	}

	by := byMods(s)

	return mapStep(func(x *exec, _ *Step, t *trav) (any, error) {
		vars := make(map[string]float64, len(names))

		for i, name := range names {
			raw, err := x.mathVariable(name, t)
			if err != nil {
				return nil, err
			}

			if len(by) > 0 {
				raw, _, err = x.applyBy(by, i, &trav{v: raw, sack: t.sack, path: t.path, loops: t.loops})
				if err != nil {
					return nil, err
				}
			}

			if !isNumber(raw) {
				return nil, malformed("The variable %q in math() must resolve to a number, found %s", name, keyOf(raw))
			}

			vars[name] = toFloat(raw)
		}

		return expr.eval(vars), nil
	})(x, s, in)
}

// mathVariable resolves a math() variable: _ is the current object, then a key of a Map current object, a path
// label, then a side-effect.
func (x *exec) mathVariable(name string, t *trav) (any, error) {
	if name == "_" {
		return t.v, nil
	}

	if m, ok := t.v.(*Map); ok {
		if v, found := m.Get(name); found {
			if l, isList := v.([]any); isList && len(l) == 1 {
				v = l[0]
			}

			return v, nil
		}
	}

	if v, ok := t.lookup(name); ok {
		return v, nil
	}

	if v, ok := x.side[name]; ok {
		return v, nil
	}

	return nil, malformed("The math() expression references the variable %q, which has no value", name)
}
