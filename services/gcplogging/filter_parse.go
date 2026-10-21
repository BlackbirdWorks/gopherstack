package gcplogging

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	maxFilterDepth = 64
	maxRegexLen    = 1000
	opHas          = ":"
	opEq           = "="
	opNe           = "!="
	opRe           = "=~"
	opNre          = "!~"
)

// filter is a compiled Logging query language expression; the nil filter matches every entry.
type filter struct {
	root node
	// hasTimestamp reports whether the expression constrains timestamp, which disables the default 24h window.
	hasTimestamp bool
}

type node interface {
	match(d *entryDoc) bool
}

type (
	andNode  struct{ kids []node }
	orNode   struct{ kids []node }
	notNode  struct{ kid node }
	textNode struct{ text string }
	cmpNode  struct {
		re    *regexp.Regexp
		field string
		op    string
		val   string
		path  []string
	}
	funcNode struct {
		name string
		args []string
	}
)

type parser struct {
	field string
	op    string
	toks  []token
	pos   int
	depth int
	nodes int
	ts    bool
}

// compileFilter parses src; an empty filter yields nil.
func compileFilter(src string) (*filter, error) {
	if strings.TrimSpace(src) == "" {
		return nil, nil //nolint:nilnil // nil filter matches everything
	}

	toks, err := lexFilter(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}

	root, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, p.unexpected()
	}

	return &filter{root: root, hasTimestamp: p.ts}, nil
}

func (f *filter) matches(d *entryDoc) bool {
	return f == nil || f.root.match(d)
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *parser) unexpected() error {
	t := p.peek()
	if t.kind == tokEOF {
		return errInvalid("Unparsable filter: unexpected end of expression.")
	}

	return errInvalid("Unparsable filter: unexpected %q at offset %d.", t.text, t.pos)
}

func (p *parser) enter() error {
	p.depth++
	p.nodes++

	if p.depth > maxFilterDepth || p.nodes > maxFilterToks {
		return errInvalid("Unparsable filter: expression is too complex.")
	}

	return nil
}

func isKeyword(t token, kw string) bool { return t.kind == tokWord && t.text == kw }

// parseAnd joins OR groups; OR binds tighter than AND, as in the Logging query language.
func (p *parser) parseAnd() (node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer func() { p.depth-- }()

	kids := make([]node, 0, 2) //nolint:mnd // typical arity

	for {
		n, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		kids = append(kids, n)

		if isKeyword(p.peek(), "AND") {
			p.next()

			continue
		}

		if !p.startsTerm() {
			break
		}
	}

	if len(kids) == 1 {
		return kids[0], nil
	}

	return &andNode{kids: kids}, nil
}

func (p *parser) startsTerm() bool {
	t := p.peek()
	if t.kind == tokWord {
		return t.text != "OR"
	}

	return t.kind != tokEOF && t.kind != tokRParen && t.kind != tokComma && t.kind != tokOp
}

// parseOr is the tighter-binding level.
func (p *parser) parseOr() (node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer func() { p.depth-- }()

	first, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	kids := []node{first}

	for isKeyword(p.peek(), "OR") {
		p.next()

		n, uerr := p.parseUnary()
		if uerr != nil {
			return nil, uerr
		}

		kids = append(kids, n)
	}

	if len(kids) == 1 {
		return first, nil
	}

	return &orNode{kids: kids}, nil
}

func (p *parser) parseUnary() (node, error) {
	t := p.peek()
	if isKeyword(t, "NOT") || t.kind == tokMinus {
		p.next()

		if err := p.enter(); err != nil {
			return nil, err
		}

		defer func() { p.depth-- }()

		kid, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &notNode{kid: kid}, nil
	}

	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	t := p.peek()

	if t.kind == tokLParen {
		p.next()

		n, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if p.next().kind != tokRParen {
			return nil, errInvalid("Unparsable filter: missing closing parenthesis.")
		}

		return n, nil
	}

	if t.kind == tokString {
		p.next()

		return p.leaf(t.text)
	}

	if t.kind == tokWord {
		return p.parseWordTerm()
	}

	return nil, p.unexpected()
}

// leaf turns a bare value into a text restriction, or inside a value group into a comparison.
func (p *parser) leaf(val string) (node, error) {
	if p.field == "" {
		return &textNode{text: strings.ToLower(val)}, nil
	}

	return p.newCmp(p.field, p.op, val)
}

func (p *parser) parseWordTerm() (node, error) {
	t := p.next()
	nt := p.peek()

	if p.field != "" {
		return p.leaf(t.text)
	}

	if nt.kind == tokLParen && !nt.spaceBefore {
		return p.parseFunc(t.text)
	}

	if nt.kind == tokOp {
		return p.parseCmp(t.text)
	}

	return &textNode{text: strings.ToLower(t.text)}, nil
}

func (p *parser) parseCmp(field string) (node, error) {
	op := p.next().text

	if strings.EqualFold(field, "timestamp") || strings.EqualFold(field, "receiveTimestamp") {
		p.ts = true
	}

	t := p.peek()

	if t.kind == tokString || t.kind == tokWord {
		p.next()

		return p.newCmp(field, op, t.text)
	}

	if t.kind != tokLParen {
		return nil, p.unexpected()
	}

	p.next()

	saveF, saveO := p.field, p.op
	p.field, p.op = field, op

	n, err := p.parseAnd()

	p.field, p.op = saveF, saveO

	if err != nil {
		return nil, err
	}

	if p.next().kind != tokRParen {
		return nil, errInvalid("Unparsable filter: missing closing parenthesis.")
	}

	return n, nil
}

func (p *parser) newCmp(field, op, val string) (node, error) {
	c := &cmpNode{field: field, op: op, val: val, path: splitPath(field)}

	if op == opRe || op == opNre {
		if len(val) > maxRegexLen {
			return nil, errInvalid("Unparsable filter: regular expression is too long.")
		}

		re, err := regexp.Compile(val)
		if err != nil {
			return nil, errInvalid("Unparsable filter: invalid regular expression %q.", val)
		}

		c.re = re
	}

	return c, nil
}

func (p *parser) parseFunc(name string) (node, error) {
	p.next()

	var args []string

	for p.peek().kind != tokRParen {
		t := p.next()
		if t.kind != tokString && t.kind != tokWord {
			return nil, errInvalid("Unparsable filter: bad argument in %s().", name)
		}

		args = append(args, t.text)

		if p.peek().kind == tokComma {
			p.next()
		}
	}

	p.next()

	return newFuncNode(name, args)
}

func newFuncNode(name string, args []string) (node, error) {
	lname := strings.ToLower(name)

	switch lname {
	case "log_id", "search":
		if len(args) != 1 {
			return nil, errInvalid("Unparsable filter: %s() takes one argument.", name)
		}
	case "sample":
		if len(args) != 2 { //nolint:mnd // field, fraction
			return nil, errInvalid("Unparsable filter: sample() takes a field and a fraction.")
		}

		f, err := strconv.ParseFloat(args[1], 64)
		if err != nil || f < 0 || f > 1 {
			return nil, errInvalid("Unparsable filter: sample() fraction must be in [0, 1].")
		}
	default:
		return nil, errInvalid("Unparsable filter: unknown function %q.", name)
	}

	return &funcNode{name: lname, args: args}, nil
}

// splitPath splits a dotted field, keeping quoted segments (labels."a.b") and a[0] indexes intact.
func splitPath(field string) []string {
	var (
		parts []string
		cur   strings.Builder
	)

	for i := 0; i < len(field); i++ {
		c := field[i]

		switch c {
		case '"':
			j := strings.IndexByte(field[i+1:], '"')
			if j < 0 {
				cur.WriteString(field[i+1:])

				i = len(field)

				break
			}

			cur.WriteString(field[i+1 : i+1+j])

			i += j + 1
		case '.':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}

	return append(parts, cur.String())
}
