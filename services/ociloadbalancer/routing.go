package ociloadbalancer

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

var errCondition = errors.New("invalid routing condition")

// condCtx is the request view a routing condition is evaluated against.
type condCtx struct {
	query   url.Values
	header  http.Header
	cookies map[string]string
	path    string
	method  string
}

func newCondCtx(r *http.Request) *condCtx {
	c := &condCtx{
		path: r.URL.Path, query: r.URL.Query(), header: r.Header, method: r.Method, cookies: map[string]string{},
	}
	for _, ck := range r.Cookies() {
		c.cookies[ck.Name] = ck.Value
	}

	return c
}

type condNode interface {
	eval(c *condCtx) bool
}

type aggNode struct {
	kids []condNode
	all  bool
}

func (n aggNode) eval(c *condCtx) bool {
	for _, k := range n.kids {
		if k.eval(c) != n.all {
			return !n.all
		}
	}

	return n.all
}

type literal struct {
	text   string
	nocase bool
}

type cmpNode struct {
	attr string
	op   string
	rhs  literal
	key  literal
}

func fold(l literal, s string) string {
	if l.nocase {
		return strings.ToLower(s)
	}

	return s
}

func (n cmpNode) value(c *condCtx) (string, bool) {
	switch n.attr {
	case "path":
		return c.path, true
	case "method":
		return c.method, true
	case "query":
		for k, v := range c.query {
			if fold(n.key, k) == fold(n.key, n.key.text) && len(v) > 0 {
				return v[0], true
			}
		}
	case "headers":
		for k, v := range c.header {
			if fold(literal{nocase: true}, k) == strings.ToLower(n.key.text) && len(v) > 0 {
				return v[0], true
			}
		}
	case "cookies":
		for k, v := range c.cookies {
			if fold(n.key, k) == fold(n.key, n.key.text) {
				return v, true
			}
		}
	}

	return "", false
}

func (n cmpNode) eval(c *condCtx) bool {
	v, ok := n.value(c)
	if !ok {
		return false
	}

	l, r := fold(n.rhs, v), fold(n.rhs, n.rhs.text)

	switch n.op {
	case "eq":
		return l == r
	case "ne":
		return l != r
	case "sw":
		return strings.HasPrefix(l, r)
	case "ew":
		return strings.HasSuffix(l, r)
	default:
		return strings.Contains(l, r)
	}
}

type condParser struct {
	src string
	pos int
}

func condErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errCondition, fmt.Sprintf(format, args...))
}

// parseCondition parses the V1 routing policy condition subset: all()/any() aggregates over comparisons of
// http.request.url.path, .url.query[k], .headers[k], .cookies[k] and .method with eq, ne, sw, ew and co.
func parseCondition(s string) (condNode, error) {
	p := &condParser{src: s}

	n, err := p.expr()
	if err != nil {
		return nil, err
	}

	p.skip()

	if p.pos != len(p.src) {
		return nil, condErr("unexpected text at offset %d", p.pos)
	}

	return n, nil
}

func (p *condParser) skip() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n') {
		p.pos++
	}
}

func (p *condParser) eat(tok string) bool {
	p.skip()

	if strings.HasPrefix(p.src[p.pos:], tok) {
		p.pos += len(tok)

		return true
	}

	return false
}

func (p *condParser) word() string {
	p.skip()

	start := p.pos
	for p.pos < len(p.src) && (p.src[p.pos] == '.' || p.src[p.pos] == '_' || isAlnum(p.src[p.pos])) {
		p.pos++
	}

	return p.src[start:p.pos]
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func (p *condParser) expr() (condNode, error) {
	save := p.pos
	w := p.word()

	if w == "all" || w == "any" {
		if !p.eat("(") {
			return nil, condErr("expected ( after %s", w)
		}

		agg := aggNode{all: w == "all"}

		for {
			k, err := p.expr()
			if err != nil {
				return nil, err
			}

			agg.kids = append(agg.kids, k)

			if p.eat(",") {
				continue
			}

			if !p.eat(")") {
				return nil, condErr("expected , or ) at offset %d", p.pos)
			}

			return agg, nil
		}
	}

	p.pos = save

	return p.comparison()
}

func (p *condParser) comparison() (condNode, error) {
	attr := p.word()
	n := cmpNode{}

	switch attr {
	case "http.request.url.path":
		n.attr = "path"
	case "http.request.method":
		n.attr = "method"
	case "http.request.url.query", "http.request.headers", "http.request.cookies":
		n.attr = attr[strings.LastIndexByte(attr, '.')+1:]

		if !p.eat("[") {
			return nil, condErr("%s needs a [key]", attr)
		}

		key, err := p.literal()
		if err != nil {
			return nil, err
		}

		if !p.eat("]") {
			return nil, condErr("expected ] at offset %d", p.pos)
		}

		n.key = key
	default:
		return nil, condErr("unknown attribute %q", attr)
	}

	op := p.word()
	switch op {
	case "eq", "ne", "sw", "ew", "co":
		n.op = op
	default:
		return nil, condErr("unknown operator %q", op)
	}

	rhs, err := p.literal()
	if err != nil {
		return nil, err
	}

	n.rhs = rhs

	return n, nil
}

func (p *condParser) literal() (literal, error) {
	if p.eat("(") {
		if p.word() != "i" {
			return literal{}, condErr("expected (i 'text')")
		}

		s, err := p.quoted()
		if err != nil {
			return literal{}, err
		}

		if !p.eat(")") {
			return literal{}, condErr("expected ) at offset %d", p.pos)
		}

		return literal{text: s, nocase: true}, nil
	}

	s, err := p.quoted()

	return literal{text: s}, err
}

func (p *condParser) quoted() (string, error) {
	p.skip()

	if p.pos >= len(p.src) || p.src[p.pos] != '\'' {
		return "", condErr("expected quoted string at offset %d", p.pos)
	}

	p.pos++

	var sb strings.Builder

	for p.pos < len(p.src) {
		c := p.src[p.pos]
		p.pos++

		switch {
		case c == '\\' && p.pos < len(p.src):
			sb.WriteByte(p.src[p.pos])
			p.pos++
		case c == '\'':
			return sb.String(), nil
		default:
			sb.WriteByte(c)
		}
	}

	return "", condErr("unterminated string")
}

const (
	matchExact   = "EXACT_MATCH"
	matchLongest = "FORCE_LONGEST_PREFIX_MATCH"
	matchPrefix  = "PREFIX_MATCH"
	matchSuffix  = "SUFFIX_MATCH"
)

func pathMatches(route PathRoute, path string) bool {
	p, r := strings.ToLower(path), strings.ToLower(route.Path)

	switch route.PathMatchType.MatchType {
	case matchExact:
		return p == r
	case matchSuffix:
		return strings.HasSuffix(p, r)
	default:
		return strings.HasPrefix(p, r)
	}
}

// routePath picks the backend set for path: EXACT_MATCH first, then the longest FORCE_LONGEST_PREFIX_MATCH,
// then the first PREFIX_MATCH or SUFFIX_MATCH in list order. Matching ignores case.
func routePath(set *PathRouteSet, path string) (string, bool) {
	for _, r := range set.PathRoutes {
		if r.PathMatchType.MatchType == matchExact && pathMatches(r, path) {
			return r.BackendSetName, true
		}
	}

	best, bestLen := "", -1

	for _, r := range set.PathRoutes {
		if r.PathMatchType.MatchType == matchLongest && pathMatches(r, path) && len(r.Path) > bestLen {
			best, bestLen = r.BackendSetName, len(r.Path)
		}
	}

	if bestLen >= 0 {
		return best, true
	}

	for _, r := range set.PathRoutes {
		if (r.PathMatchType.MatchType == matchPrefix || r.PathMatchType.MatchType == matchSuffix) &&
			pathMatches(r, path) {
			return r.BackendSetName, true
		}
	}

	return "", false
}
