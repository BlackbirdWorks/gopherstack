package azuregraph

import (
	"strconv"
	"strings"
)

const advancedHint = "Try adding $count=true query parameter and ConsistencyLevel:eventual header. " +
	"Refer to https://aka.ms/graph-docs/advanced-queries for more information"

type tokKind int

const (
	tokIdent tokKind = iota
	tokString
	tokLParen
	tokRParen
	tokComma
	tokColon
	tokEOF
)

type token struct {
	text string
	kind tokKind
}

func lexFilter(s string) ([]token, bool) {
	var out []token

	for i := 0; i < len(s); {
		c := s[i]

		switch {
		case c == ' ' || c == '\t':
			i++
		case c == '(':
			out, i = append(out, token{"(", tokLParen}), i+1
		case c == ')':
			out, i = append(out, token{")", tokRParen}), i+1
		case c == ',':
			out, i = append(out, token{",", tokComma}), i+1
		case c == ':':
			out, i = append(out, token{":", tokColon}), i+1
		case c == '\'':
			var sb strings.Builder

			i++

			for {
				if i >= len(s) {
					return nil, false
				}

				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						sb.WriteByte('\'')

						i += 2

						continue
					}

					i++

					break
				}

				sb.WriteByte(s[i])

				i++
			}

			out = append(out, token{sb.String(), tokString})
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t(),:'", rune(s[j])) {
				j++
			}

			if j == i {
				return nil, false
			}

			out, i = append(out, token{s[i:j], tokIdent}), j
		}
	}

	return append(out, token{kind: tokEOF}), true
}

// filterNode is a compiled $filter expression.
type filterNode func(obj map[string]any, scope map[string]any) bool

type filterParser struct {
	err      *Error
	resource string
	toks     []token
	props    []string
	vars     []string
	pos      int
	advanced string
}

// compiledFilter is a parsed $filter plus whether it needs advanced-query support.
type compiledFilter struct {
	match    filterNode
	advanced string
}

func compileFilter(expr, resource string, fields map[string]bool) (*compiledFilter, error) {
	toks, ok := lexFilter(expr)
	bad := unsupported("Unsupported or invalid query filter clause specified for resource '%s'.", resource)

	if !ok {
		return nil, bad
	}

	p := &filterParser{toks: toks, resource: resource}
	n := p.parseOr()

	if p.err != nil || p.cur().kind != tokEOF {
		if p.err != nil {
			return nil, p.err
		}

		return nil, bad
	}

	for _, prop := range p.props {
		if !fields[prop] {
			return nil, unsupported("Unsupported or invalid query filter clause specified for property '%s' of resource '%s'.",
				prop, resource)
		}
	}

	return &compiledFilter{match: n, advanced: p.advanced}, nil
}

func (p *filterParser) cur() token { return p.toks[p.pos] }

func (p *filterParser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *filterParser) fail() filterNode {
	if p.err == nil {
		p.err = unsupported("Unsupported or invalid query filter clause specified for resource '%s'.", p.resource)
	}

	return func(map[string]any, map[string]any) bool { return false }
}

func (p *filterParser) isWord(w string) bool {
	return p.cur().kind == tokIdent && strings.EqualFold(p.cur().text, w)
}

func (p *filterParser) parseOr() filterNode {
	l := p.parseAnd()

	for p.isWord("or") {
		p.next()

		r := p.parseAnd()
		prev := l
		l = func(o, s map[string]any) bool { return prev(o, s) || r(o, s) }
	}

	return l
}

func (p *filterParser) parseAnd() filterNode {
	l := p.parseUnary()

	for p.isWord("and") {
		p.next()

		r := p.parseUnary()
		prev := l
		l = func(o, s map[string]any) bool { return prev(o, s) && r(o, s) }
	}

	return l
}

func (p *filterParser) parseUnary() filterNode {
	if p.isWord("not") {
		p.next()

		p.advanced = "not"

		inner := p.parseUnary()

		return func(o, s map[string]any) bool { return !inner(o, s) }
	}

	if p.cur().kind == tokLParen {
		p.next()

		n := p.parseOr()
		if p.next().kind != tokRParen {
			return p.fail()
		}

		return n
	}

	return p.parsePrimary()
}

func (p *filterParser) isVar(path string) bool {
	head, _, _ := strings.Cut(path, "/")

	for _, v := range p.vars {
		if v == head {
			return true
		}
	}

	return false
}

func (p *filterParser) parsePrimary() filterNode {
	t := p.next()
	if t.kind != tokIdent {
		return p.fail()
	}

	if fn := strings.ToLower(t.text); (fn == "startswith" || fn == "endswith" || fn == "contains") && p.cur().kind == tokLParen {
		return p.parseFunc(fn)
	}

	path := t.text
	if strings.HasSuffix(strings.ToLower(path), "/any") && p.cur().kind == tokLParen {
		return p.parseAny(strings.TrimSuffix(path, path[len(path)-4:]))
	}

	isv := p.isVar(path)
	if !isv {
		head, _, _ := strings.Cut(path, "/")
		p.props = append(p.props, head)
	}

	op := strings.ToLower(p.next().text)
	if !slicesContainsStr([]string{"eq", "ne", "gt", "ge", "lt", "le"}, op) {
		return p.fail()
	}

	if op == "ne" {
		p.advanced = "ne"
	}

	lit, ok := p.parseLiteral()
	if !ok {
		return p.fail()
	}

	return func(o, s map[string]any) bool { return compareValues(resolve(path, isv, o, s), op, lit) }
}

func slicesContainsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}

	return false
}

func resolve(path string, isVar bool, obj, scope map[string]any) any {
	root := obj
	if isVar {
		root = scope
	}

	return lookupPath(root, path)
}

func lookupPath(root map[string]any, path string) any {
	var cur any = root

	for _, seg := range strings.Split(path, "/") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}

		found := false

		for k, v := range m {
			if strings.EqualFold(k, seg) {
				cur, found = v, true

				break
			}
		}

		if !found {
			return nil
		}
	}

	return cur
}

func (p *filterParser) parseLiteral() (any, bool) {
	t := p.next()

	switch t.kind {
	case tokString:
		return t.text, true
	case tokIdent:
		switch strings.ToLower(t.text) {
		case "true":
			return true, true
		case "false":
			return false, true
		case "null":
			return nil, true
		}

		if f, err := strconv.ParseFloat(t.text, 64); err == nil {
			return f, true
		}

		return t.text, true
	}

	return nil, false
}

func (p *filterParser) parseFunc(fn string) filterNode {
	p.next() // (

	pathTok := p.next()
	if pathTok.kind != tokIdent || p.next().kind != tokComma {
		return p.fail()
	}

	lit, ok := p.parseLiteral()
	if !ok || p.next().kind != tokRParen {
		return p.fail()
	}

	if fn == "endswith" || fn == "contains" {
		p.advanced = fn
	}

	isv := p.isVar(pathTok.text)
	if !isv {
		head, _, _ := strings.Cut(pathTok.text, "/")
		p.props = append(p.props, head)
	}

	want, _ := lit.(string)

	return func(o, s map[string]any) bool {
		got, isStr := resolve(pathTok.text, isv, o, s).(string)
		if !isStr {
			return false
		}

		g, w := strings.ToLower(got), strings.ToLower(want)

		switch fn {
		case "startswith":
			return strings.HasPrefix(g, w)
		case "endswith":
			return strings.HasSuffix(g, w)
		}

		return strings.Contains(g, w)
	}
}

func (p *filterParser) parseAny(path string) filterNode {
	p.next() // (

	v := p.next()
	if v.kind != tokIdent || p.next().kind != tokColon {
		return p.fail()
	}

	head, _, _ := strings.Cut(path, "/")
	p.props = append(p.props, head)
	p.vars = append(p.vars, v.text)
	inner := p.parseOr()
	p.vars = p.vars[:len(p.vars)-1]

	if p.next().kind != tokRParen {
		return p.fail()
	}

	return func(o, s map[string]any) bool {
		list, _ := lookupPath(o, path).([]any)

		for _, item := range list {
			if inner(o, map[string]any{v.text: item}) {
				return true
			}
		}

		return false
	}
}

func compareValues(got any, op string, want any) bool {
	if want == nil {
		isNull := got == nil || got == ""

		return isNull == (op == "eq")
	}

	if _, isList := got.([]any); isList || got == nil {
		return op == "ne"
	}

	var c int

	switch w := want.(type) {
	case string:
		g, ok := got.(string)
		if !ok {
			return op == "ne"
		}

		c = strings.Compare(strings.ToLower(g), strings.ToLower(w))
	case bool:
		g, ok := got.(bool)
		if !ok {
			return op == "ne"
		}

		if g != w {
			c = 1
		}
	case float64:
		g, ok := got.(float64)
		if !ok {
			return op == "ne"
		}

		switch {
		case g < w:
			c = -1
		case g > w:
			c = 1
		}
	}

	switch op {
	case "eq":
		return c == 0
	case "ne":
		return c != 0
	case "gt":
		return c > 0
	case "ge":
		return c >= 0
	case "lt":
		return c < 0
	}

	return c <= 0
}
