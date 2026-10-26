package gcpcompute

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

type filterNode interface {
	eval(doc map[string]any) bool
}

type (
	andNode  struct{ l, r filterNode }
	orNode   struct{ l, r filterNode }
	notNode  struct{ n filterNode }
	compNode struct {
		re    *regexp.Regexp
		field []string
		op    string
		value string
	}
)

func (n andNode) eval(d map[string]any) bool { return n.l.eval(d) && n.r.eval(d) }
func (n orNode) eval(d map[string]any) bool  { return n.l.eval(d) || n.r.eval(d) }
func (n notNode) eval(d map[string]any) bool { return !n.n.eval(d) }

func lookup(doc map[string]any, path []string) []string {
	return walk(doc, path)
}

func walk(cur any, path []string) []string {
	if len(path) == 0 {
		return scalars(cur)
	}

	switch t := cur.(type) {
	case map[string]any:
		next, ok := t[path[0]]
		if !ok {
			return nil
		}

		return walk(next, path[1:])
	case []any:
		var out []string
		for _, e := range t {
			out = append(out, walk(e, path)...)
		}

		return out
	}

	return nil
}

func scalars(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return []string{t}
	case bool:
		return []string{strconv.FormatBool(t)}
	case float64:
		return []string{strconv.FormatFloat(t, 'f', -1, 64)}
	case []any:
		var out []string
		for _, e := range t {
			out = append(out, scalars(e)...)
		}

		return out
	}

	return nil
}

func cmpValues(a, b string) int {
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)

	if errA == nil && errB == nil {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}

		return 0
	}

	return strings.Compare(a, b)
}

func (n compNode) matches(have string) bool {
	switch n.op {
	case "=", "eq", "!=", "ne":
		return n.re.MatchString(have)
	case ":":
		return n.value == "*" || strings.Contains(have, n.value)
	case ">":
		return cmpValues(have, n.value) > 0
	case ">=":
		return cmpValues(have, n.value) >= 0
	case "<":
		return cmpValues(have, n.value) < 0
	case "<=":
		return cmpValues(have, n.value) <= 0
	}

	return false
}

func (n compNode) eval(doc map[string]any) bool {
	vals := lookup(doc, n.field)
	negate := n.op == "!=" || n.op == "ne"

	if len(vals) == 0 {
		return negate
	}

	hit := false

	for _, v := range vals {
		if n.matches(v) {
			hit = true

			break
		}
	}

	return hit != negate
}

type filterParser struct {
	toks []string
	pos  int
}

func tokenize(s string) ([]string, bool) {
	var toks []string

	for i := 0; i < len(s); {
		c := s[i]

		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == '(' || c == ')':
			toks = append(toks, string(c))
			i++
		case c == '"' || c == '\'':
			j := strings.IndexByte(s[i+1:], c)
			if j < 0 {
				return nil, false
			}

			toks = append(toks, "\x00"+s[i+1:i+1+j])
			i += j + 2
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t\n()", rune(s[j])) {
				j++
			}

			toks = append(toks, s[i:j])
			i = j
		}
	}

	return toks, true
}

func (p *filterParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return ""
}

func (p *filterParser) next() string {
	t := p.peek()
	p.pos++

	return t
}

func (p *filterParser) parseOr() (filterNode, bool) {
	l, ok := p.parseAnd()
	for ok && p.peek() == "OR" {
		p.next()

		var r filterNode
		if r, ok = p.parseAnd(); ok {
			l = orNode{l, r}
		}
	}

	return l, ok
}

func (p *filterParser) parseAnd() (filterNode, bool) {
	l, ok := p.parseUnary()

	for ok {
		t := p.peek()
		if t == "" || t == ")" || t == "OR" {
			break
		}

		if t == "AND" {
			p.next()
		}

		var r filterNode
		if r, ok = p.parseUnary(); ok {
			l = andNode{l, r}
		}
	}

	return l, ok
}

func (p *filterParser) parseUnary() (filterNode, bool) {
	switch p.peek() {
	case "NOT", "-":
		p.next()

		n, ok := p.parseUnary()

		return notNode{n}, ok
	case "(":
		p.next()

		n, ok := p.parseOr()
		if !ok || p.next() != ")" {
			return nil, false
		}

		return n, true
	case "", ")", "AND", "OR":
		return nil, false
	}

	return p.parseComp()
}

var filterOps = []string{"!=", ">=", "<=", "=", ">", "<", ":"}

// splitFused splits "field=value" style tokens written without spaces around the operator.
func splitFused(tok string) (field, op, rest string, ok bool) {
	i := strings.IndexAny(tok, "=!<>:")
	if i <= 0 {
		return "", "", "", false
	}

	for _, o := range filterOps {
		if strings.HasPrefix(tok[i:], o) {
			return tok[:i], o, tok[i+len(o):], true
		}
	}

	return "", "", "", false
}

func (p *filterParser) parseComp() (filterNode, bool) {
	field := p.next()
	if strings.HasPrefix(field, "\x00") {
		return nil, false
	}

	var op, val string

	if f, o, rest, fused := splitFused(field); fused {
		field, op, val = f, o, strings.Trim(rest, `"'`)
		if val == "" {
			val = p.next()
		}
	} else {
		op, val = p.next(), p.next()
	}

	if op == "" || val == "" {
		return nil, false
	}

	val = strings.TrimPrefix(val, "\x00")

	n := compNode{field: strings.Split(field, "."), op: op, value: val}

	switch op {
	case "=", "eq", "!=", "ne":
		re, err := regexp.Compile("^(?:" + val + ")$")
		if err != nil {
			return nil, false
		}

		n.re = re
	case ":", ">", "<", ">=", "<=":
	default:
		return nil, false
	}

	return n, true
}

func parseFilter(expr string) (filterNode, *APIError) {
	if strings.TrimSpace(expr) == "" {
		return nil, nil
	}

	toks, ok := tokenize(expr)
	if !ok {
		return nil, errBadFilter(expr)
	}

	p := &filterParser{toks: toks}

	n, ok := p.parseOr()
	if !ok || p.pos < len(p.toks) {
		return nil, errBadFilter(expr)
	}

	return n, nil
}

func toDoc(item any) map[string]any {
	data, err := json.Marshal(item)
	if err != nil {
		return nil
	}

	var m map[string]any
	_ = json.Unmarshal(data, &m)

	return m
}
