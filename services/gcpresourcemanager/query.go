package gcpresourcemanager

import (
	"slices"
	"strings"
	"unicode"
)

// fieldFunc returns the values of a searchable field; ok is false for an unknown field.
const (
	fieldDisplayName = "displayname"
	fieldState       = "state"
	fieldLabels      = "labels"
	kwAnd            = "AND"
	kwOr             = "OR"
)

type fieldFunc func(field string) (vals []string, ok bool)

type query struct {
	root     node
	hasState bool
}

type node interface {
	eval(fields fieldFunc) (bool, error)
}

type (
	termNode struct{ field, value string }
	notNode  struct{ n node }
	binNode  struct {
		l, r node
		and  bool
	}
)

func (t termNode) eval(fields fieldFunc) (bool, error) {
	if t.field == "" {
		for _, f := range []string{fieldDisplayName, "id", fieldLabels} {
			vals, _ := fields(f)
			if slices.ContainsFunc(vals, func(v string) bool { return strings.Contains(strings.ToLower(v), t.value) }) {
				return true, nil
			}
		}

		return false, nil
	}

	vals, ok := fields(t.field)
	if !ok {
		return false, errInvalid("Invalid query: unknown field %q.", t.field)
	}

	return slices.ContainsFunc(vals, func(v string) bool { return glob(t.value, strings.ToLower(v)) }), nil
}

func (n notNode) eval(fields fieldFunc) (bool, error) {
	v, err := n.n.eval(fields)

	return !v, err
}

func (n binNode) eval(fields fieldFunc) (bool, error) {
	l, err := n.l.eval(fields)
	if err != nil {
		return false, err
	}

	if l == !n.and {
		return l, nil
	}

	return n.r.eval(fields)
}

// glob matches s against pattern, where * stands for any run of characters.
func glob(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}

	if !strings.HasPrefix(s, parts[0]) {
		return false
	}

	s = s[len(parts[0]):]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}

		s = s[i+len(mid):]
	}

	return strings.HasSuffix(s, parts[len(parts)-1])
}

func tokenize(q string) []string {
	var (
		out    []string
		cur    strings.Builder
		quoted bool
	)

	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
		}
	}

	rs := []rune(q)
	for i := 0; i < len(rs); i++ {
		c := rs[i]

		switch {
		case c == '\\' && i+1 < len(rs) && rs[i+1] == '"':
			cur.WriteRune('"')

			i++
		case c == '"':
			quoted = !quoted
		case unicode.IsSpace(c) && !quoted:
			flush()
		default:
			cur.WriteRune(c)
		}
	}

	flush()

	return out
}

type parser struct {
	toks        []string
	pos         int
	hasState    bool
	implicitAnd bool
}

func (p *parser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return ""
}

// parseQuery reads terms joined by AND, OR and NOT; terms with no operator between them combine with AND when
// implicitAnd is set and OR otherwise.
func parseQuery(q string, implicitAnd bool) (*query, error) {
	toks := tokenize(q)
	if len(toks) == 0 {
		return &query{}, nil
	}

	p := &parser{toks: toks, implicitAnd: implicitAnd}

	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.pos < len(p.toks) {
		return nil, errInvalid("Invalid query: unexpected %q.", p.peek())
	}

	return &query{root: root, hasState: p.hasState}, nil
}

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.toks) {
		explicit := p.peek() == kwOr
		if !explicit && (p.implicitAnd || p.peek() == kwAnd) {
			break
		}

		if explicit {
			p.pos++
		}

		r, rErr := p.parseAnd()
		if rErr != nil {
			return nil, rErr
		}

		l = binNode{l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.toks) {
		explicit := p.peek() == kwAnd
		if !explicit && (!p.implicitAnd || p.peek() == kwOr) {
			break
		}

		if explicit {
			p.pos++
		}

		r, rErr := p.parseNot()
		if rErr != nil {
			return nil, rErr
		}

		l = binNode{l: l, r: r, and: true}
	}

	return l, nil
}

func (p *parser) parseNot() (node, error) {
	if p.peek() == "NOT" {
		p.pos++

		n, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return notNode{n}, nil
	}

	tok := p.peek()
	if tok == "" || tok == kwAnd || tok == kwOr {
		return nil, errInvalid("Invalid query: expected a term.")
	}

	p.pos++

	field, value := "", tok

	if i := strings.IndexAny(tok, ":="); i > 0 {
		field, value = strings.ToLower(tok[:i]), tok[i+1:]
	}

	if field == fieldState || field == "lifecyclestate" {
		p.hasState = true
		field = fieldState
	}

	return termNode{field: field, value: strings.ToLower(value)}, nil
}

// match reports whether the query selects a resource with the given fields. An empty query selects all.
func (q *query) match(fields fieldFunc) (bool, error) {
	if q.root == nil {
		return true, nil
	}

	return q.root.eval(fields)
}
