package gcpcloudbuild

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var errUnboundVariable = errors.New("variable is not defined")

// celExpr is a compiled trigger filter over substitution variables.
type celExpr func(vars map[string]string) (celVal, error)

type celVal struct {
	s      string
	b      bool
	isBool bool
}

type celParser struct {
	src string
	pos int
}

// compileFilter compiles the subset of CEL trigger filters the emulator evaluates: string literals,
// substitution variables, == != && || ! parentheses and the matches/contains/startsWith/endsWith methods.
func compileFilter(src string) (celExpr, error) {
	p := &celParser{src: src}

	e, err := p.or()
	if err != nil {
		return nil, err
	}

	if p.skip(); p.pos != len(p.src) {
		return nil, fmt.Errorf("unexpected %q at offset %d", p.src[p.pos:], p.pos)
	}

	return e, nil
}

// evalFilter reports whether the filter accepts vars.
func evalFilter(e celExpr, vars map[string]string) (bool, error) {
	v, err := e(vars)
	if err != nil {
		return false, err
	}

	if !v.isBool {
		return false, errors.New("filter must evaluate to a bool")
	}

	return v.b, nil
}

func (p *celParser) skip() {
	for p.pos < len(p.src) && strings.ContainsRune(" \t\r\n", rune(p.src[p.pos])) {
		p.pos++
	}
}

func (p *celParser) eat(tok string) bool {
	p.skip()

	if strings.HasPrefix(p.src[p.pos:], tok) {
		p.pos += len(tok)

		return true
	}

	return false
}

func (p *celParser) or() (celExpr, error) {
	l, err := p.and()
	if err != nil {
		return nil, err
	}

	for p.eat("||") {
		r, rErr := p.and()
		if rErr != nil {
			return nil, rErr
		}

		l = logic(l, r, true)
	}

	return l, nil
}

func (p *celParser) and() (celExpr, error) {
	l, err := p.unary()
	if err != nil {
		return nil, err
	}

	for p.eat("&&") {
		r, rErr := p.unary()
		if rErr != nil {
			return nil, rErr
		}

		l = logic(l, r, false)
	}

	return l, nil
}

func logic(l, r celExpr, isOr bool) celExpr {
	return func(vars map[string]string) (celVal, error) {
		a, err := asBool(l, vars)
		if err != nil || a == isOr {
			return celVal{b: a, isBool: true}, err
		}

		c, err := asBool(r, vars)

		return celVal{b: c, isBool: true}, err
	}
}

func asBool(e celExpr, vars map[string]string) (bool, error) {
	v, err := e(vars)
	if err != nil {
		return false, err
	}

	if !v.isBool {
		return false, errors.New("logical operands must be bool")
	}

	return v.b, nil
}

func (p *celParser) unary() (celExpr, error) {
	p.skip()

	if p.pos < len(p.src) && p.src[p.pos] == '!' && !strings.HasPrefix(p.src[p.pos:], "!=") {
		p.pos++

		e, err := p.unary()
		if err != nil {
			return nil, err
		}

		return func(vars map[string]string) (celVal, error) {
			v, vErr := asBool(e, vars)

			return celVal{b: !v, isBool: true}, vErr
		}, nil
	}

	return p.compare()
}

func (p *celParser) compare() (celExpr, error) {
	l, err := p.primary()
	if err != nil {
		return nil, err
	}

	op := ""

	switch {
	case p.eat("=="):
		op = "=="
	case p.eat("!="):
		op = "!="
	default:
		return l, nil
	}

	r, err := p.primary()
	if err != nil {
		return nil, err
	}

	return func(vars map[string]string) (celVal, error) {
		a, aErr := l(vars)
		if aErr != nil {
			return celVal{}, aErr
		}

		c, cErr := r(vars)
		if cErr != nil {
			return celVal{}, cErr
		}

		if a.isBool != c.isBool {
			return celVal{}, errors.New("cannot compare a string with a bool")
		}

		eq := a == c

		return celVal{b: eq == (op == "=="), isBool: true}, nil
	}, nil
}

func (p *celParser) primary() (celExpr, error) {
	p.skip()

	if p.pos >= len(p.src) {
		return nil, errors.New("unexpected end of filter")
	}

	var (
		e   celExpr
		err error
	)

	switch c := p.src[p.pos]; {
	case c == '(':
		p.pos++

		if e, err = p.or(); err != nil {
			return nil, err
		}

		if !p.eat(")") {
			return nil, errors.New("missing closing parenthesis")
		}
	case c == '"' || c == '\'':
		var s string

		if s, err = p.str(); err != nil {
			return nil, err
		}

		e = func(map[string]string) (celVal, error) { return celVal{s: s}, nil }
	case c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		e = p.ident()
	default:
		return nil, fmt.Errorf("unexpected %q at offset %d", string(c), p.pos)
	}

	return p.methods(e)
}

func (p *celParser) ident() celExpr {
	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] == '_' || p.src[p.pos] >= '0' && p.src[p.pos] <= '9' ||
		p.src[p.pos] >= 'A' && p.src[p.pos] <= 'Z' || p.src[p.pos] >= 'a' && p.src[p.pos] <= 'z') {
		p.pos++
	}

	name := p.src[start:p.pos]

	switch name {
	case "true", "false":
		return func(map[string]string) (celVal, error) { return celVal{b: name == "true", isBool: true}, nil }
	}

	return func(vars map[string]string) (celVal, error) {
		v, ok := vars[name]
		if !ok {
			return celVal{}, fmt.Errorf("%q: %w", name, errUnboundVariable)
		}

		return celVal{s: v}, nil
	}
}

func (p *celParser) str() (string, error) {
	q := p.src[p.pos]
	p.pos++

	var sb strings.Builder

	for p.pos < len(p.src) {
		c := p.src[p.pos]
		p.pos++

		switch {
		case c == q:
			return sb.String(), nil
		case c == '\\' && p.pos < len(p.src):
			sb.WriteByte(p.src[p.pos])
			p.pos++
		default:
			sb.WriteByte(c)
		}
	}

	return "", errors.New("unterminated string literal")
}

func (p *celParser) methods(recv celExpr) (celExpr, error) {
	for p.eat(".") {
		p.skip()

		start := p.pos
		for p.pos < len(p.src) && (p.src[p.pos] >= 'a' && p.src[p.pos] <= 'z' || p.src[p.pos] >= 'A' && p.src[p.pos] <= 'Z') {
			p.pos++
		}

		name := p.src[start:p.pos]

		if !p.eat("(") {
			return nil, fmt.Errorf("field selection .%s is not supported", name)
		}

		p.skip()

		if p.pos >= len(p.src) || p.src[p.pos] != '"' && p.src[p.pos] != '\'' {
			return nil, fmt.Errorf("%s() takes a string literal", name)
		}

		arg, err := p.str()
		if err != nil {
			return nil, err
		}

		if !p.eat(")") {
			return nil, errors.New("missing closing parenthesis")
		}

		if recv, err = method(recv, name, arg); err != nil {
			return nil, err
		}
	}

	return recv, nil
}

func method(recv celExpr, name, arg string) (celExpr, error) {
	var test func(s string) bool

	switch name {
	case "matches":
		re, err := regexp.Compile(arg)
		if err != nil {
			return nil, fmt.Errorf("matches(): %w", err)
		}

		test = re.MatchString
	case "contains":
		test = func(s string) bool { return strings.Contains(s, arg) }
	case "startsWith":
		test = func(s string) bool { return strings.HasPrefix(s, arg) }
	case "endsWith":
		test = func(s string) bool { return strings.HasSuffix(s, arg) }
	default:
		return nil, fmt.Errorf("method %s() is not supported", name)
	}

	return func(vars map[string]string) (celVal, error) {
		v, err := recv(vars)
		if err != nil {
			return celVal{}, err
		}

		if v.isBool {
			return celVal{}, fmt.Errorf("%s() needs a string receiver", name)
		}

		return celVal{b: test(v.s), isBool: true}, nil
	}, nil
}
