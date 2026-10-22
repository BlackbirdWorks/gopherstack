package gremlin

import (
	"strconv"
)

const maxParseDepth = 64

// Stmt is one script statement: a traversal and the terminal call that ends it.
type Stmt struct {
	Tr   *Traversal
	Term string
	N    int
}

type parser struct {
	bindings map[string]any
	toks     []token
	pos      int
	depth    int
}

// ParseScript parses a Gremlin-Groovy script made of g.<traversal> statements.
func ParseScript(src string, bindings map[string]any) ([]Stmt, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks, bindings: bindings}

	var stmts []Stmt

	for p.peek().kind != tEOF {
		if p.acceptPunct(";") {
			continue
		}

		s, serr := p.parseStmt()
		if serr != nil {
			return nil, serr
		}

		stmts = append(stmts, s)
	}

	if len(stmts) == 0 {
		return nil, malformed("Script is empty")
	}

	return stmts, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) peekNext() token {
	const n = 1

	if p.pos+n >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}

	return p.toks[p.pos+n]
}

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tEOF {
		p.pos++
	}

	return t
}

func (p *parser) isPunct(s string) bool {
	t := p.peek()

	return t.kind == tPunct && t.text == s
}

func (p *parser) acceptPunct(s string) bool {
	if p.isPunct(s) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) expectPunct(s string) error {
	if !p.acceptPunct(s) {
		return p.unexpected()
	}

	return nil
}

func (p *parser) unexpected() error {
	t := p.peek()
	if t.kind == tEOF {
		return malformed("Unexpected end of script at offset %d", t.pos)
	}

	return malformed("Unexpected token '%s' at offset %d", t.text, t.pos)
}

func (p *parser) parseStmt() (Stmt, error) {
	t := p.next()
	if t.kind != tIdent || t.text != "g" {
		return Stmt{}, malformed("No such property: %s for class: Script1", t.text)
	}

	tr := &Traversal{}

	term, n, err := p.parseChain(tr, true)
	if err != nil {
		return Stmt{}, err
	}

	if len(tr.Steps) == 0 {
		return Stmt{}, malformed("Unexpected end of script at offset %d", p.peek().pos)
	}

	return Stmt{Tr: tr, Term: term, N: n}, nil
}

func isTerminal(name string) bool {
	switch name {
	case "toList", "next", "iterate", "hasNext", "toSet", "tryNext", "getRawTraversal", "fill":
		return true
	}

	return false
}

// parseChain reads ".step(args)" calls into tr. At top level it also reads the terminal call.
func (p *parser) parseChain(tr *Traversal, top bool) (string, int, error) {
	for p.isPunct(".") {
		p.pos++

		name := p.next()
		if name.kind != tIdent {
			return "", 0, malformed("Unexpected token '%s' at offset %d", name.text, name.pos)
		}

		if err := p.expectPunct("("); err != nil {
			return "", 0, err
		}

		args, err := p.parseArgs()
		if err != nil {
			return "", 0, err
		}

		if top && isTerminal(name.text) {
			return name.text, termCount(args), nil
		}

		if skipSourceStep(name.text) {
			continue
		}

		tr.add(Step{Name: name.text, Args: args})
	}

	return "", 0, nil
}

func skipSourceStep(name string) bool {
	switch name {
	case "withSideEffect", "withStrategies", "withoutStrategies", "withBulk", "withPath", "withSack":
		return true
	}

	return false
}

func termCount(args []any) int {
	if len(args) == 1 && isInt(args[0]) {
		return int(toInt(args[0]))
	}

	return 0
}

func (p *parser) parseArgs() ([]any, error) {
	var args []any

	if p.acceptPunct(")") {
		return args, nil
	}

	for {
		a, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		args = append(args, a)

		if p.acceptPunct(")") {
			return args, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) parseValue() (any, error) {
	p.depth++
	defer func() { p.depth-- }()

	if p.depth > maxParseDepth {
		return nil, malformed("Script nesting is too deep")
	}

	t := p.peek()

	switch t.kind {
	case tString:
		p.pos++

		return t.text, nil
	case tInt, tFloat:
		p.pos++

		return numberValue(t)
	case tPunct:
		return p.parsePunctValue(t)
	case tIdent:
		return p.parseIdentValue()
	case tEOF:
	}

	return nil, p.unexpected()
}

func (p *parser) parsePunctValue(t token) (any, error) {
	switch t.text {
	case "-":
		p.pos++

		n := p.next()
		if n.kind != tInt && n.kind != tFloat {
			return nil, malformed("Unexpected token '%s' at offset %d", n.text, n.pos)
		}

		v, err := numberValue(n)
		if err != nil {
			return nil, err
		}

		if isInt(v) {
			return -toInt(v), nil
		}

		return -toFloat(v), nil
	case "[":
		p.pos++

		return p.parseCollection()
	}

	return nil, p.unexpected()
}

func numberValue(t token) (any, error) {
	if t.kind == tInt {
		i, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, malformed("Number out of range: %s", t.text)
		}

		return i, nil
	}

	f, err := strconv.ParseFloat(t.text, 64)
	if err != nil {
		return nil, malformed("Invalid number: %s", t.text)
	}

	return f, nil
}

// parseCollection reads a Groovy list [a, b] or map [k:v] after the opening bracket.
func (p *parser) parseCollection() (any, error) {
	if p.acceptPunct("]") {
		return []any{}, nil
	}

	if p.isPunct(":") && p.peekNext().text == "]" {
		p.pos += 2

		return NewMap(), nil
	}

	first, err := p.parseMapKeyOrValue()
	if err != nil {
		return nil, err
	}

	if p.acceptPunct(":") {
		return p.parseMapRest(first)
	}

	list := []any{first}

	for !p.acceptPunct("]") {
		if err = p.expectPunct(","); err != nil {
			return nil, err
		}

		v, verr := p.parseValue()
		if verr != nil {
			return nil, verr
		}

		list = append(list, v)
	}

	return list, nil
}

// parseMapKeyOrValue reads a bare map key (Groovy treats [a:1] keys as strings) or a value.
func (p *parser) parseMapKeyOrValue() (any, error) {
	t := p.peek()
	if t.kind == tIdent && p.peekNext().text == ":" && p.peekNext().kind == tPunct {
		p.pos++

		return t.text, nil
	}

	return p.parseValue()
}

func (p *parser) parseMapRest(firstKey any) (any, error) {
	m := NewMap()
	key := firstKey

	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		m.Put(key, v)

		if p.acceptPunct("]") {
			return m, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}

		if key, err = p.parseMapKeyOrValue(); err != nil {
			return nil, err
		}

		if err = p.expectPunct(":"); err != nil {
			return nil, err
		}
	}
}

func (p *parser) parseIdentValue() (any, error) {
	t := p.next()

	switch t.text {
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "null":
		return nil, nil //nolint:nilnil // null literal
	}

	if p.isPunct(".") && p.peekNext().kind == tIdent {
		return p.parseQualified(t)
	}

	if p.isPunct("(") {
		p.pos++

		return p.parseCall(t.text, false)
	}

	if tok, ok := bareToken(t.text); ok {
		return tok, nil
	}

	if v, ok := p.bindings[t.text]; ok {
		return v, nil
	}

	return nil, malformed("No such property: %s for class: Script1", t.text)
}

// parseQualified reads P.gt(1), __.out(), T.id, Order.desc, VertexProperty.Cardinality.single, g.V().
func (p *parser) parseQualified(head token) (any, error) {
	switch head.text {
	case "__", "g":
		tr := &Traversal{}

		if _, _, err := p.parseChain(tr, false); err != nil {
			return nil, err
		}

		return tr, nil
	case "P", "TextP":
		p.pos++
		name := p.next()

		if err := p.expectPunct("("); err != nil {
			return nil, err
		}

		return p.parseCall(name.text, true)
	}

	return p.parseEnumRef(head)
}

func (p *parser) parseEnumRef(head token) (any, error) {
	kind := head.text
	p.pos++

	name := p.next()
	if kind == "VertexProperty" && name.text == "Cardinality" && p.isPunct(".") {
		p.pos++
		name = p.next()
		kind = kindCardinality
	}

	if kind == "WithOptions" {
		return Token{Kind: kindWithOptions, Name: name.text}, nil
	}

	if t, ok := enumToken(kind, name.text); ok {
		return t, nil
	}

	return nil, malformed("No such property: %s for class: %s", name.text, kind)
}

// parseCall parses a predicate or an anonymous traversal step after "name(".
func (p *parser) parseCall(name string, forcePred bool) (any, error) {
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	if forcePred || isPredicateName(name) {
		return p.parsePredChain(newPred(name, args))
	}

	tr := &Traversal{}
	tr.add(Step{Name: name, Args: args})

	if _, _, err = p.parseChain(tr, false); err != nil {
		return nil, err
	}

	return tr, nil
}

func (p *parser) parsePredChain(pr Pred) (any, error) {
	for p.isPunct(".") && p.peekNext().kind == tIdent {
		op := p.peekNext().text
		if op != opAnd && op != opOr && op != "negate" {
			break
		}

		p.pos += 2
		if err := p.expectPunct("("); err != nil {
			return nil, err
		}

		args, err := p.parseArgs()
		if err != nil {
			return nil, err
		}

		pr = combinePred(op, pr, args)
	}

	return pr, nil
}

func combinePred(op string, pr Pred, args []any) Pred {
	if op == "negate" {
		return Pred{Op: "not", Children: []Pred{pr}}
	}

	kids := []Pred{pr}

	for _, a := range args {
		if c, ok := a.(Pred); ok {
			kids = append(kids, c)
		}
	}

	return Pred{Op: op, Children: kids}
}
