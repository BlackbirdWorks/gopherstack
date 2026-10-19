package gcpkms

import (
	"cmp"
	"slices"
	"strings"
	"time"
	"unicode"
)

type filterOp int

const (
	opEq filterOp = iota + 1
	opNe
	opHas
	opLt
	opLe
	opGt
	opGe
)

// listSpec describes what a List RPC's filter and order_by may reference on item type T.
type listSpec[T any] struct {
	get    func(item T, field string) string
	order  map[string]func(a, b T) int
	fields []string
	times  []string
}

type condition struct {
	field string
	value string
	op    filterOp
}

// filterNode is an OR of AND-groups; each group is a conjunction of conditions or a parenthesised sub-expression.
type filterNode struct {
	cond *condition
	sub  *filterNode
	or   [][]*filterNode
}

func canonField(f string) string {
	if strings.HasPrefix(f, "labels.") {
		return f
	}

	return strings.ToLower(strings.ReplaceAll(f, "_", ""))
}

func tokenize(s string) ([]string, error) {
	var toks []string

	rs := []rune(s)

	for i := 0; i < len(rs); {
		var (
			tok  string
			next int
			err  error
		)

		switch r := rs[i]; {
		case unicode.IsSpace(r):
			i++

			continue
		case strings.ContainsRune("():=", r):
			tok, next = string(r), i+1
		case strings.ContainsRune("!<>", r):
			tok, next = lexRelational(rs, i)
		case r == '"':
			tok, next, err = lexQuoted(rs, i)
		default:
			tok, next = lexWord(rs, i, len(toks) > 0 && opOf(toks[len(toks)-1]) != 0)
		}

		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		i = next
	}

	return toks, nil
}

func lexRelational(rs []rune, i int) (string, int) {
	if i+1 < len(rs) && rs[i+1] == '=' {
		return string(rs[i : i+len("<=")]), i + len("<=")
	}

	return string(rs[i]), i + 1
}

func lexQuoted(rs []rune, i int) (string, int, error) {
	j := i + 1
	for j < len(rs) && rs[j] != '"' {
		j++
	}

	if j >= len(rs) {
		return "", 0, errInvalid("filter: unterminated quoted string.")
	}

	return "\"" + string(rs[i+1:j]), j + 1, nil
}

// lexWord reads a bare word; after an operator a value may contain ':' (e.g. RFC 3339 times).
func lexWord(rs []rune, i int, isValue bool) (string, int) {
	stops := "():=!<>\""
	if isValue {
		stops = "()\""
	}

	j := i
	for j < len(rs) && !unicode.IsSpace(rs[j]) && !strings.ContainsRune(stops, rs[j]) {
		j++
	}

	return string(rs[i:j]), j
}

type filterParser[T any] struct {
	spec *listSpec[T]
	toks []string
	pos  int
}

func (p *filterParser[T]) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return ""
}

func (p *filterParser[T]) next() string {
	t := p.peek()
	p.pos++

	return t
}

func (p *filterParser[T]) parseExpr() (*filterNode, error) {
	n := &filterNode{}

	for {
		group, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		n.or = append(n.or, group)

		if p.peek() != "OR" {
			return n, nil
		}

		p.pos++
	}
}

func (p *filterParser[T]) parseAnd() ([]*filterNode, error) {
	var group []*filterNode

	for {
		f, err := p.parseFactor()
		if err != nil {
			return nil, err
		}

		group = append(group, f)

		if p.peek() == "AND" {
			p.pos++

			continue
		}

		return group, nil
	}
}

func (p *filterParser[T]) parseFactor() (*filterNode, error) {
	if p.peek() == "(" {
		p.pos++

		sub, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if p.next() != ")" {
			return nil, errInvalid("filter: missing closing parenthesis.")
		}

		return &filterNode{sub: sub}, nil
	}

	c, err := p.parseCondition()
	if err != nil {
		return nil, err
	}

	return &filterNode{cond: c}, nil
}

func opOf(tok string) filterOp {
	return map[string]filterOp{"=": opEq, "!=": opNe, ":": opHas, "<": opLt, "<=": opLe, ">": opGt, ">=": opGe}[tok]
}

func (p *filterParser[T]) parseCondition() (*condition, error) {
	field := p.next()
	if field == "" || strings.ContainsAny(field, "\"") || field == "AND" || field == "OR" || field == "NOT" {
		return nil, errInvalid("filter: expected a field name.")
	}

	field = canonField(field)
	if !p.knownField(field) {
		return nil, errInvalid("filter: field %q is not supported; supported fields: %s.",
			field, strings.Join(p.spec.fields, ", "))
	}

	op := opOf(p.next())
	if op == 0 {
		return nil, errInvalid("filter: unsupported operator after %q; use =, !=, :, <, <=, > or >=.", field)
	}

	val := p.next()
	if val == "" || val == "(" || val == ")" {
		return nil, errInvalid("filter: expected a value after the operator.")
	}

	val = strings.TrimPrefix(val, "\"")

	if op >= opLt {
		if !slices.Contains(p.spec.times, field) {
			return nil, errInvalid("filter: ordering operators are only supported on timestamp fields.")
		}

		if _, err := time.Parse(time.RFC3339Nano, val); err != nil {
			return nil, errInvalid("filter: %q is not an RFC 3339 timestamp.", val)
		}
	}

	return &condition{field: field, op: op, value: val}, nil
}

func (p *filterParser[T]) knownField(f string) bool {
	if strings.HasPrefix(f, "labels.") {
		return slices.Contains(p.spec.fields, "labels.*") && len(f) > len("labels.")
	}

	return slices.Contains(p.spec.fields, f)
}

func (c *condition) match(got string) bool {
	switch c.op {
	case opEq:
		return got == c.value
	case opNe:
		return got != c.value
	case opHas:
		return strings.Contains(got, c.value)
	case opLt, opLe, opGt, opGe:
		return c.matchTime(got)
	}

	return false
}

func (c *condition) matchTime(got string) bool {
	have, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		return false
	}

	want, _ := time.Parse(time.RFC3339Nano, c.value)
	d := have.Compare(want)

	switch c.op {
	case opLt:
		return d < 0
	case opLe:
		return d <= 0
	case opGt:
		return d > 0
	case opGe:
		return d >= 0
	default:
		return false
	}
}

func (n *filterNode) eval(get func(field string) string) bool {
	switch {
	case n.cond != nil:
		return n.cond.match(get(n.cond.field))
	case n.sub != nil:
		return n.sub.eval(get)
	}

	for _, group := range n.or {
		if allMatch(group, get) {
			return true
		}
	}

	return false
}

func allMatch(group []*filterNode, get func(string) string) bool {
	for _, f := range group {
		if !f.eval(get) {
			return false
		}
	}

	return true
}

func parseFilter[T any](spec *listSpec[T], s string) (*filterNode, error) {
	toks, err := tokenize(s)
	if err != nil {
		return nil, err
	}

	p := &filterParser[T]{spec: spec, toks: toks}

	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if p.pos < len(toks) {
		return nil, errInvalid("filter: unexpected token %q.", toks[p.pos])
	}

	return n, nil
}

const maxOrderWords = 2

type orderTerm[T any] struct {
	cmp  func(a, b T) int
	desc bool
}

func parseOrderBy[T any](spec *listSpec[T], s string) ([]orderTerm[T], error) {
	var terms []orderTerm[T]

	for part := range strings.SplitSeq(s, ",") {
		words := strings.Fields(part)
		if len(words) == 0 || len(words) > maxOrderWords {
			return nil, errInvalid("order_by: invalid term %q.", part)
		}

		fn, ok := spec.order[canonField(words[0])]
		if !ok {
			return nil, errInvalid("order_by: field %q is not supported; use name or create_time.", words[0])
		}

		term := orderTerm[T]{cmp: fn}

		if len(words) > 1 {
			switch strings.ToLower(words[1]) {
			case "desc":
				term.desc = true
			case "asc":
			default:
				return nil, errInvalid("order_by: direction must be asc or desc, got %q.", words[1])
			}
		}

		terms = append(terms, term)
	}

	return terms, nil
}

// applyListOptions filters then orders items per a List request's filter and order_by; empty strings are no-ops.
func applyListOptions[T any](items []T, filter, orderBy string, spec *listSpec[T]) ([]T, error) {
	var (
		root  *filterNode
		terms []orderTerm[T]
		err   error
	)

	if strings.TrimSpace(filter) != "" {
		if root, err = parseFilter(spec, filter); err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(orderBy) != "" {
		if terms, err = parseOrderBy(spec, orderBy); err != nil {
			return nil, err
		}
	}

	if root != nil {
		items = slices.DeleteFunc(slices.Clone(items), func(it T) bool {
			return !root.eval(func(f string) string { return spec.get(it, f) })
		})
	}

	slices.SortStableFunc(items, func(a, b T) int {
		for _, t := range terms {
			if c := t.cmp(a, b); c != 0 {
				if t.desc {
					return -c
				}

				return c
			}
		}

		return 0
	})

	return items, nil
}

func byString[T any](f func(T) string) func(a, b T) int {
	return func(a, b T) int { return cmp.Compare(f(a), f(b)) }
}

func byTime[T any](f func(T) time.Time) func(a, b T) int {
	return func(a, b T) int { return f(a).Compare(f(b)) }
}
