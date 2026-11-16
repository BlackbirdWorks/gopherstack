package azuresearch

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	litTrue  = "true"
	quoteLen = 2
)

const skillConditional = "#Microsoft.Skills.Util.ConditionalSkill"

// evalExpression evaluates the skillset expression language used by "= ..." input sources: $(path) references,
// literals, comparisons, && || and !. ok is false when the expression is malformed.
func evalExpression(src string, t enrichTree, ctx, concrete string) (any, bool) {
	e := &exprParser{
		s:        strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(src), "=")),
		t:        t,
		ctx:      ctx,
		concrete: concrete,
	}

	v, ok := e.or()
	e.skip()

	return v, ok && e.i == len(e.s)
}

type exprParser struct {
	t        enrichTree
	s        string
	ctx      string
	concrete string
	i        int
}

func (e *exprParser) skip() {
	for e.i < len(e.s) && e.s[e.i] == ' ' {
		e.i++
	}
}

func (e *exprParser) eat(tok string) bool {
	e.skip()

	if strings.HasPrefix(e.s[e.i:], tok) {
		e.i += len(tok)

		return true
	}

	return false
}

func (e *exprParser) or() (any, bool) {
	l, ok := e.and()

	for ok && e.eat("||") {
		r, rok := e.and()
		l, ok = truthy(l) || truthy(r), rok
	}

	return l, ok
}

func (e *exprParser) and() (any, bool) {
	l, ok := e.cmp()

	for ok && e.eat("&&") {
		r, rok := e.cmp()
		l, ok = truthy(l) && truthy(r), rok
	}

	return l, ok
}

func (e *exprParser) cmp() (any, bool) {
	l, ok := e.unary()
	if !ok {
		return nil, false
	}

	for _, op := range []string{"==", "!=", "<=", ">=", "<", ">"} {
		if e.eat(op) {
			r, rok := e.unary()

			return compareExpr(op, l, r), rok
		}
	}

	return l, true
}

func (e *exprParser) unary() (any, bool) {
	if e.eat("!") && !strings.HasPrefix(e.s[e.i:], "=") {
		v, ok := e.unary()

		return !truthy(v), ok
	}

	return e.atom()
}

func (e *exprParser) atom() (any, bool) {
	e.skip()

	if e.i >= len(e.s) {
		return nil, false
	}

	rest := e.s[e.i:]

	switch {
	case e.eat("("):
		v, ok := e.or()

		return v, ok && e.eat(")")
	case strings.HasPrefix(rest, "$("):
		end := strings.IndexByte(rest, ')')
		if end < 0 {
			return nil, false
		}

		e.i += end + 1
		v, _ := e.t.get(bind(rest[2:end], e.ctx, e.concrete))

		return v, true
	case rest[0] == '\'' || rest[0] == '"':
		end := strings.IndexByte(rest[1:], rest[0])
		if end < 0 {
			return nil, false
		}

		e.i += end + quoteLen

		return rest[1 : end+1], true
	}

	return e.word(rest)
}

func (e *exprParser) word(rest string) (any, bool) {
	n := 0
	for n < len(rest) && !strings.ContainsRune(" &|=!<>()", rune(rest[n])) {
		n++
	}

	w := rest[:n]
	e.i += n

	switch w {
	case litTrue:
		return true, true
	case "false":
		return false, true
	case "null":
		return nil, true
	}

	f, err := strconv.ParseFloat(w, 64)

	return f, err == nil
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(x, litTrue)
	}

	return false
}

func compareExpr(op string, l, r any) bool {
	lf, lok := asFloat(l)
	rf, rok := asFloat(r)

	if lok && rok {
		switch op {
		case "==":
			return lf == rf
		case "!=":
			return lf != rf
		case "<":
			return lf < rf
		case ">":
			return lf > rf
		case "<=":
			return lf <= rf
		}

		return lf >= rf
	}

	ls, rs := fmt.Sprint(l), fmt.Sprint(r)

	switch op {
	case "==":
		return ls == rs
	case "!=":
		return ls != rs
	case "<":
		return ls < rs
	case ">":
		return ls > rs
	case "<=":
		return ls <= rs
	}

	return ls >= rs
}

// execConditional implements Util.ConditionalSkill: output is whenTrue or whenFalse by condition.
func execConditional(sk map[string]any, t enrichTree, ctx, concrete string) error {
	cond, _ := skillInput(sk, t, "condition", ctx, concrete)

	name := "whenFalse"
	if truthy(cond) {
		name = "whenTrue"
	}

	if v, ok := skillInput(sk, t, name, ctx, concrete); ok {
		skillOutput(sk, t, "output", v, concrete)
	}

	return nil
}
