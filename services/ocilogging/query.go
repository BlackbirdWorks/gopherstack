package ocilogging

import (
	"cmp"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Pipeline operators and special fields of the Logging query language.
const (
	kindWhere   = "where"
	kindSort    = "sort"
	kindTop     = "top"
	kindLimit   = "limit"
	kindCount   = "count"
	fieldTime   = "datetime"
	fieldRegion = "regionId"
	fieldRaw    = "logContent"
	twoRunes    = 2
)

// Token kinds of the Logging query language.
const (
	tkEOF = iota
	tkPath
	tkString
	tkNumber
	tkPunct
)

type token struct {
	text   string
	path   []string
	kind   int
	quoted bool
}

func isWordStart(r rune) bool { return unicode.IsLetter(r) || r == '_' || r == '$' }

func isWordPart(r rune) bool { return isWordStart(r) || unicode.IsDigit(r) }

// lex splits a query into tokens; a path is a dotted chain of words and double-quoted segments.
func lex(src string) ([]token, *APIError) {
	rs, i, out := []rune(src), 0, []token{}

	for i < len(rs) {
		if unicode.IsSpace(rs[i]) {
			i++

			continue
		}

		t, n, aerr := lexToken(rs, i)
		if aerr != nil {
			return nil, aerr
		}

		out, i = append(out, t), n
	}

	return append(out, token{kind: tkEOF}), nil
}

func numberEnd(rs []rune, i int) int {
	for i < len(rs) && (unicode.IsDigit(rs[i]) || rs[i] == '.') {
		i++
	}

	return i
}

// lexToken reads the token that starts at rs[i] and returns the index after it.
func lexToken(rs []rune, i int) (token, int, *APIError) {
	r := rs[i]

	switch {
	case r == '\'':
		s, n, ok := readQuoted(rs, i, '\'')
		if !ok {
			return token{}, 0, errParam("unterminated string literal in search query")
		}

		return token{kind: tkString, text: s}, n, nil
	case r == '"' || isWordStart(r):
		p, n, aerr := readPath(rs, i)
		if aerr != nil {
			return token{}, 0, aerr
		}

		return token{
			kind: tkPath, path: p, text: strings.Join(p, "."), quoted: r == '"' && rs[n-1] == '"' && len(p) == 1,
		}, n, nil
	case unicode.IsDigit(r) || (r == '-' && i+1 < len(rs) && unicode.IsDigit(rs[i+1])):
		n := numberEnd(rs, i+1)

		return token{kind: tkNumber, text: string(rs[i:n])}, n, nil
	}

	t, n := readPunct(rs, i)
	if t == "" {
		return token{}, 0, errParam(fmt.Sprintf("unexpected character %q in search query", r))
	}

	return token{kind: tkPunct, text: t}, n, nil
}

func readQuoted(rs []rune, start int, q rune) (string, int, bool) {
	var sb strings.Builder

	for i := start + 1; i < len(rs); i++ {
		switch {
		case rs[i] == '\\' && i+1 < len(rs):
			i++
			sb.WriteRune(rs[i])
		case rs[i] == q:
			return sb.String(), i + 1, true
		default:
			sb.WriteRune(rs[i])
		}
	}

	return "", 0, false
}

func readPath(rs []rune, start int) ([]string, int, *APIError) {
	var path []string

	i := start

	for {
		switch {
		case i < len(rs) && rs[i] == '"':
			s, n, ok := readQuoted(rs, i, '"')
			if !ok {
				return nil, 0, errParam("unterminated quoted identifier in search query")
			}

			path, i = append(path, s), n
		case i < len(rs) && isWordStart(rs[i]):
			j := i
			for j < len(rs) && isWordPart(rs[j]) {
				j++
			}

			path, i = append(path, string(rs[i:j])), j
		default:
			return nil, 0, errParam("malformed identifier in search query")
		}

		if i < len(rs) && rs[i] == '.' {
			i++

			continue
		}

		return path, i, nil
	}
}

func readPunct(rs []rune, i int) (string, int) {
	two := ""
	if i+1 < len(rs) {
		two = string(rs[i : i+twoRunes])
	}

	switch two {
	case "!=", "<=", ">=", "<>":
		return two, i + twoRunes
	}

	if strings.ContainsRune("=<>(),|", rs[i]) {
		return string(rs[i]), i + 1
	}

	return "", i
}

// Query is a parsed Logging search.
type Query struct {
	Streams []string
	ops     []qop
}

type qop struct {
	where expr
	kind  string
	keys  []sortKey
	n     int
}

type sortKey struct {
	path []string
	desc bool
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *parser) isWord(w string) bool {
	t := p.peek()

	return t.kind == tkPath && len(t.path) == 1 && strs.Equal(t.path[0], w)
}

func (p *parser) isPunct(s string) bool {
	t := p.peek()

	return t.kind == tkPunct && t.text == s
}

var unsupportedOps = []string{ //nolint:gochecknoglobals // immutable keyword list
	"select", "extend", "dedup", "summarize", "fields", "head", "tail", "rename", "timestats", "join", "union",
	"distinct", "stats", "sample", "highlight", "parse",
}

// ParseQuery parses the supported subset of the Logging query language.
func ParseQuery(src string) (*Query, error) {
	toks, aerr := lex(src)
	if aerr != nil {
		return nil, aerr
	}

	p := &parser{toks: toks}
	if !p.isWord("search") {
		return nil, errParam(`a search query must start with search "<compartment>[/<logGroup>[/<log>]]"`)
	}

	p.next()

	q := &Query{}

	for {
		t := p.next()
		if t.kind != tkString && !t.quoted {
			return nil, errParam(`search needs a quoted scope such as "<compartment>/<logGroup>/<log>"`)
		}

		q.Streams = append(q.Streams, t.text)

		if !p.isPunct(",") {
			break
		}

		p.next()
	}

	for p.isPunct("|") {
		p.next()

		o, err := p.parseOp()
		if err != nil {
			return nil, err
		}

		q.ops = append(q.ops, o)
	}

	if p.peek().kind != tkEOF {
		return nil, errParam("unexpected " + strconv.Quote(p.peek().text) + " in search query")
	}

	return q, nil
}

func (p *parser) parseOp() (qop, *APIError) {
	t := p.peek()
	if t.kind == tkPath && len(t.path) == 1 {
		w := strings.ToLower(t.path[0])

		switch {
		case slices.Contains(unsupportedOps, w):
			return qop{}, errParam("the " + w + " operator is not supported by this emulator")
		case w == kindSort:
			return p.parseSort()
		case w == kindTop:
			return p.parseTop()
		case w == kindLimit:
			p.next()

			n, err := p.parseCount()

			return qop{kind: kindLimit, n: n}, err
		case w == kindCount:
			p.next()

			return qop{kind: kindCount}, nil
		case w == kindWhere:
			p.next()
		}
	}

	e, err := p.parseOr()

	return qop{kind: kindWhere, where: e}, err
}

func (p *parser) parseCount() (int, *APIError) {
	t := p.next()

	n, err := strconv.Atoi(t.text)
	if t.kind != tkNumber || err != nil || n < 0 {
		return 0, errParam("a non-negative integer is required after limit/top")
	}

	return n, nil
}

// parseKey reads a field and an optional direction; desc applies when none is given.
func (p *parser) parseKey(desc bool) (sortKey, *APIError) {
	t := p.next()
	if t.kind != tkPath {
		return sortKey{}, errParam("a field name is required after sort by/top ... by")
	}

	k := sortKey{path: t.path, desc: desc}

	switch {
	case p.isWord("desc"):
		p.next()

		k.desc = true
	case p.isWord("asc"):
		p.next()

		k.desc = false
	}

	return k, nil
}

func (p *parser) parseSort() (qop, *APIError) {
	p.next()

	if !p.isWord("by") {
		return qop{}, errParam("sort must be followed by by")
	}

	p.next()

	o := qop{kind: kindSort}

	for {
		k, err := p.parseKey(false)
		if err != nil {
			return qop{}, err
		}

		o.keys = append(o.keys, k)

		if !p.isPunct(",") {
			return o, nil
		}

		p.next()
	}
}

func (p *parser) parseTop() (qop, *APIError) {
	p.next()

	n, err := p.parseCount()
	if err != nil {
		return qop{}, err
	}

	if !p.isWord("by") {
		return qop{}, errParam("top N must be followed by by <field>")
	}

	p.next()

	k, err := p.parseKey(true)
	if err != nil {
		return qop{}, err
	}

	return qop{kind: kindTop, n: n, keys: []sortKey{k}}, nil
}

// Expressions.

type expr interface{ eval(r *row) bool }

type orExpr struct{ l, r expr }

type andExpr struct{ l, r expr }

type notExpr struct{ e expr }

type cmpExpr struct {
	lit  any
	op   string
	path []string
	set  []any
}

func (e orExpr) eval(r *row) bool  { return e.l.eval(r) || e.r.eval(r) }
func (e andExpr) eval(r *row) bool { return e.l.eval(r) && e.r.eval(r) }
func (e notExpr) eval(r *row) bool { return !e.e.eval(r) }

func (p *parser) parseOr() (expr, *APIError) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isWord("or") {
		p.next()

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = orExpr{l, r}
	}

	return l, nil
}

func (p *parser) parseAnd() (expr, *APIError) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isWord("and") {
		p.next()

		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = andExpr{l, r}
	}

	return l, nil
}

func (p *parser) parseNot() (expr, *APIError) {
	if p.isWord("not") {
		p.next()

		e, err := p.parseNot()

		return notExpr{e}, err
	}

	if p.isPunct("(") {
		p.next()

		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if !p.isPunct(")") {
			return nil, errParam("missing ) in search query")
		}

		p.next()

		return e, nil
	}

	return p.parseCmp()
}

func (p *parser) parseLiteral() (any, *APIError) {
	t := p.next()

	switch {
	case t.kind == tkString:
		return t.text, nil
	case t.kind == tkNumber:
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, errParam("invalid number " + t.text)
		}

		return f, nil
	case t.kind == tkPath && len(t.path) == 1 && strs.Equal(t.path[0], "true"):
		return true, nil
	case t.kind == tkPath && len(t.path) == 1 && strs.Equal(t.path[0], "false"):
		return false, nil
	case t.kind == tkPath && len(t.path) == 1 && strs.Equal(t.path[0], "null"):
		return nil, nil
	}

	return nil, errParam("a string, number, true, false or null literal is required, got " + strconv.Quote(t.text))
}

func (p *parser) parseCmp() (expr, *APIError) {
	t := p.next()
	if t.kind != tkPath {
		return nil, errParam("expected a field name in search query, got " + strconv.Quote(t.text))
	}

	c := cmpExpr{path: t.path}
	nt := p.peek()

	switch {
	case nt.kind == tkPunct && slices.Contains([]string{"=", "!=", "<>", "<", "<=", ">", ">="}, nt.text):
		p.next()

		c.op = nt.text
		if c.op == "<>" {
			c.op = "!="
		}

		lit, err := p.parseLiteral()
		c.lit = lit

		return c, err
	case p.isWord("in"):
		p.next()

		if !p.isPunct("(") {
			return nil, errParam("in must be followed by a parenthesised list")
		}

		p.next()

		c.op = "in"

		for {
			lit, err := p.parseLiteral()
			if err != nil {
				return nil, err
			}

			c.set = append(c.set, lit)

			if p.isPunct(",") {
				p.next()

				continue
			}

			break
		}

		if !p.isPunct(")") {
			return nil, errParam("missing ) after in list")
		}

		p.next()

		return c, nil
	}

	c.op = "truthy"

	return c, nil
}

// Rows.

type row struct {
	ce  map[string]any
	raw string
	ms  float64
	seq int64
}

func (r *row) lookup(path []string) (any, bool) {
	switch {
	case path[0] == fieldTime && len(path) == 1:
		return r.ms, true
	case path[0] == fieldRegion && len(path) == 1:
		return Region, true
	case path[0] == fieldRaw && len(path) == 1:
		return r.raw, true
	case path[0] == fieldRaw:
		path = path[1:]
	}

	var cur any = r.ce

	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		if cur, ok = m[seg]; !ok {
			return nil, false
		}
	}

	return cur, true
}

func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return ""
	}

	b, _ := json.Marshal(v)

	return string(b)
}

func wildcard(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	for i := range parts {
		parts[i] = regexp.QuoteMeta(parts[i])
	}

	return regexp.MustCompile("(?s)^" + strings.Join(parts, ".*") + "$").MatchString(s)
}

func equalVal(v, lit any) bool {
	switch l := lit.(type) {
	case string:
		if strings.Contains(l, "*") {
			return wildcard(l, scalarString(v))
		}

		return scalarString(v) == l
	case float64:
		f, ok := toFloat(v)

		return ok && f == l
	case bool:
		b, ok := v.(bool)

		return ok && b == l
	}

	return v == nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)

		return f, err == nil
	}

	return 0, false
}

func orderVal(v, lit any) (int, bool) {
	if l, isNum := lit.(float64); isNum {
		f, numeric := toFloat(v)

		return cmp.Compare(f, l), numeric
	}

	if l, isStr := lit.(string); isStr {
		return strings.Compare(scalarString(v), l), v != nil
	}

	return 0, false
}

func (c cmpExpr) eval(r *row) bool {
	v, present := r.lookup(c.path)

	switch c.op {
	case "truthy":
		b, isBool := v.(bool)

		return present && isBool && b
	case "=", "!=", "in":
		return c.evalEquality(v, present)
	}

	if !present {
		return false
	}

	d, ordered := orderVal(v, c.lit)
	if !ordered {
		return false
	}

	switch c.op {
	case "<":
		return d < 0
	case "<=":
		return d <= 0
	case ">":
		return d > 0
	}

	return d >= 0
}

func (c cmpExpr) evalEquality(v any, present bool) bool {
	switch c.op {
	case "=":
		return (present && equalVal(v, c.lit)) || (!present && c.lit == nil)
	case "!=":
		return (!present || !equalVal(v, c.lit)) && (present || c.lit != nil)
	}

	return present && slices.ContainsFunc(c.set, func(l any) bool { return equalVal(v, l) })
}

func compareRows(a, c *row, keys []sortKey) int {
	for _, k := range keys {
		va, oka := a.lookup(k.path)
		vc, okc := c.lookup(k.path)

		var d int

		switch {
		case !oka || !okc:
			d = cmp.Compare(b2i(oka), b2i(okc))
		default:
			fa, fok := toFloat(va)
			fc, cok := toFloat(vc)

			if _, isStr := va.(string); fok && cok && !isStr {
				d = cmp.Compare(fa, fc)
			} else {
				d = strings.Compare(scalarString(va), scalarString(vc))
			}
		}

		if k.desc {
			d = -d
		}

		if d != 0 {
			return d
		}
	}

	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}

	return 0
}

// apply runs the pipeline over rows; count replaces the rows by a single count row.
func (q *Query) apply(rows []*row) ([]*row, *int) {
	for _, o := range q.ops {
		switch o.kind {
		case kindWhere:
			rows = slices.DeleteFunc(rows, func(r *row) bool { return !o.where.eval(r) })
		case kindSort:
			slices.SortStableFunc(rows, func(a, c *row) int { return compareRows(a, c, o.keys) })
		case kindTop:
			slices.SortStableFunc(rows, func(a, c *row) int { return compareRows(a, c, o.keys) })
			rows = rows[:min(o.n, len(rows))]
		case kindLimit:
			rows = rows[:min(o.n, len(rows))]
		case kindCount:
			n := len(rows)

			return nil, &n
		}
	}

	return rows, nil
}
