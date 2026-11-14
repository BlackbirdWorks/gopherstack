package sparql

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Format is an RDF serialization.
type Format uint8

// RDF formats.
const (
	FormatNTriples Format = iota
	FormatNQuads
	FormatTurtle
	FormatTriG
	FormatRDFXML
)

// ParseOptions tunes RDF parsing.
type ParseOptions struct {
	// Base resolves relative IRIs (Neptune's loader default is http://aws.amazon.com/neptune/default).
	Base string
	// Graph receives triples that name no graph; DefaultGraph when unset.
	Graph Term
	// KeepBlankLabels keeps blank-node labels verbatim instead of scoping them to this document.
	KeepBlankLabels bool
}

// ParseError is a syntax error at a record (statement or line) of an RDF document.
type ParseError struct {
	Msg    string
	Record int
}

func (e *ParseError) Error() string { return e.Msg }

func (o ParseOptions) graph() Term {
	if o.Graph.Bound() {
		return o.Graph
	}

	return IRI(DefaultGraph)
}

func (o ParseOptions) base() string {
	if o.Base != "" {
		return o.Base
	}

	return DefaultBase
}

// ParseRDF parses a whole document; on error it returns the quads read before it together with a *ParseError.
func ParseRDF(f Format, data []byte, opts ParseOptions) ([]Quad, error) {
	qs, errs := ParseRDFLenient(f, data, opts, false)
	if len(errs) > 0 {
		return qs, errs[0]
	}

	return qs, nil
}

// ParseRDFLenient parses a document. With skip set, line-based formats resume after a bad line; other formats
// stop at their first error.
func ParseRDFLenient(f Format, data []byte, opts ParseOptions, skip bool) ([]Quad, []*ParseError) {
	doc := ""
	if !opts.KeepBlankLabels {
		doc = strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	}

	switch f {
	case FormatRDFXML:
		qs, err := parseRDFXML(data, opts, doc)
		if err != nil {
			return qs, []*ParseError{asParseError(err, 0)}
		}

		return qs, nil
	case FormatNTriples, FormatNQuads:
		return parseLines(f, string(data), opts, doc, skip)
	default:
		return parseTurtleDoc(string(data), opts, doc, f == FormatTriG)
	}
}

func asParseError(err error, record int) *ParseError {
	if pe, ok := err.(*ParseError); ok { //nolint:errorlint // own concrete type, never wrapped
		return pe
	}

	return &ParseError{Msg: err.Error(), Record: record}
}

func parseLines(f Format, src string, opts ParseOptions, doc string, skip bool) ([]Quad, []*ParseError) {
	var (
		out  []Quad
		errs []*ParseError
	)

	n := 0

	for i, line := range strings.Split(src, "\n") {
		if t := strings.TrimSpace(line); t == "" || strings.HasPrefix(t, "#") {
			continue
		}

		n++

		qs, err := parseTurtleLine(line, f == FormatNQuads, opts, doc)
		if err != nil {
			errs = append(errs, &ParseError{Msg: err.Error(), Record: i + 1})

			if !skip {
				return out, errs
			}

			continue
		}

		out = append(out, qs...)
	}

	return out, errs
}

func parseTurtleLine(line string, quads bool, opts ParseOptions, doc string) ([]Quad, error) {
	tp, err := newTurtle(line, opts, doc)
	if err != nil {
		return nil, err
	}

	tp.quads = quads

	if err = tp.document(false); err != nil {
		return nil, err
	}

	return tp.out, nil
}

func parseTurtleDoc(src string, opts ParseOptions, doc string, trig bool) ([]Quad, []*ParseError) {
	tp, err := newTurtle(src, opts, doc)
	if err != nil {
		return nil, []*ParseError{asParseError(err, 0)}
	}

	if err = tp.document(trig); err != nil {
		return tp.out, []*ParseError{{Msg: err.Error(), Record: tp.record}}
	}

	return tp.out, nil
}

type turtle struct {
	*parser
	doc    string
	graph  Term
	out    []Quad
	opts   ParseOptions
	record int
	quads  bool
}

func newTurtle(src string, opts ParseOptions, doc string) (*turtle, error) {
	p, err := newParser(src)
	if err != nil {
		return nil, err
	}

	p.base = opts.base()

	return &turtle{parser: p, opts: opts, doc: doc, graph: opts.graph()}, nil
}

func (t *turtle) document(trig bool) error {
	for t.peek().kind != tEOF {
		t.record++

		if err := t.statement(trig); err != nil {
			return err
		}
	}

	return nil
}

func (t *turtle) statement(trig bool) error {
	tok := t.peek()

	switch {
	case tok.kind == tLang && strings.EqualFold(tok.val, "prefix"):
		t.i++

		return t.prefixDecl(true)
	case tok.kind == tLang && strings.EqualFold(tok.val, "base"):
		t.i++

		return t.baseDecl(true)
	case tok.kw("PREFIX"):
		t.i++

		return t.prefixDecl(false)
	case tok.kw("BASE"):
		t.i++

		return t.baseDecl(false)
	case trig && (tok.kw("GRAPH") || t.isGraphBlockStart()):
		return t.graphBlock()
	case trig && tok.is("{"):
		return t.block(t.opts.graph())
	}

	return t.triples()
}

func (t *turtle) isGraphBlockStart() bool {
	k := t.peek().kind

	return (k == tIRI || k == tPName || k == tBlank) && t.peekNext().is("{")
}

func (t *turtle) prefixDecl(dot bool) error {
	name := t.advance()
	if name.kind != tPName || !strings.HasSuffix(name.val, ":") {
		return t.errf("expected prefix name")
	}

	iri := t.advance()
	if iri.kind != tIRI {
		return t.errf("expected IRI in prefix declaration")
	}

	t.prefixes[strings.TrimSuffix(name.val, ":")] = t.resolve(iri.val)

	if dot {
		return t.expectP(".")
	}

	return nil
}

func (t *turtle) baseDecl(dot bool) error {
	iri := t.advance()
	if iri.kind != tIRI {
		return t.errf("expected IRI in base declaration")
	}

	t.base = t.resolve(iri.val)

	if dot {
		return t.expectP(".")
	}

	return nil
}

func (t *turtle) graphBlock() error {
	t.acceptKW("GRAPH")

	var g Term

	switch tok := t.peek(); tok.kind {
	case tBlank:
		t.i++

		g = t.blankTerm(tok.val)
	default:
		iri, err := t.iriTok()
		if err != nil {
			return err
		}

		g = iri
	}

	return t.block(g)
}

func (t *turtle) block(g Term) error {
	if err := t.expectP("{"); err != nil {
		return err
	}

	prev := t.graph
	t.graph = g

	defer func() { t.graph = prev }()

	for !t.peek().is("}") {
		if t.peek().kind == tEOF {
			return t.errf("unterminated graph block")
		}

		if err := t.triples(); err != nil {
			return err
		}
	}

	t.i++

	return nil
}

func (t *turtle) blankTerm(label string) Term { return Blank(t.doc + label) }

func (t *turtle) newBlank() Term {
	t.bn++

	return Blank(t.doc + "g" + strconv.Itoa(t.bn))
}

func (t *turtle) emit(s, p, o, g Term) {
	t.out = append(t.out, Quad{S: s, P: p, O: o, G: g})
}

func (t *turtle) triples() error {
	var (
		subj Term
		err  error
	)

	if t.peek().is("[") && !t.peekNext().is("]") {
		t.i++
		subj = t.newBlank()

		if err = t.predicateObjectList(subj); err != nil {
			return err
		}

		if err = t.expectP("]"); err != nil {
			return err
		}

		if t.acceptP(".") {
			return nil
		}
	} else if subj, err = t.subjectTerm(); err != nil {
		return err
	}

	if err = t.predicateObjectList(subj); err != nil {
		return err
	}

	if t.peek().is("}") {
		return nil
	}

	return t.expectP(".")
}

func (t *turtle) subjectTerm() (Term, error) {
	tok := t.peek()

	switch {
	case tok.kind == tBlank:
		t.i++

		return t.blankTerm(tok.val), nil
	case tok.is("("):
		return t.collection()
	case tok.is("[") && t.peekNext().is("]"):
		t.i += 2

		return t.newBlank(), nil
	default:
		return t.iriTok()
	}
}

func (t *turtle) collection() (Term, error) {
	t.i++

	var items []Term

	for !t.peek().is(")") {
		if t.peek().kind == tEOF {
			return Term{}, t.errf("unterminated collection")
		}

		o, err := t.objectTerm()
		if err != nil {
			return Term{}, err
		}

		items = append(items, o)
	}

	t.i++

	if len(items) == 0 {
		return IRI(RDFNil), nil
	}

	head := t.newBlank()
	cur := head

	for i, it := range items {
		t.emit(cur, IRI(RDFFirst), it, t.graph)

		if i == len(items)-1 {
			t.emit(cur, IRI(RDFRest), IRI(RDFNil), t.graph)

			break
		}

		next := t.newBlank()
		t.emit(cur, IRI(RDFRest), next, t.graph)
		cur = next
	}

	return head, nil
}

func (t *turtle) predicateObjectList(subj Term) error {
	for {
		var pred Term

		if t.acceptKW("a") {
			pred = IRI(RDFType)
		} else {
			iri, err := t.iriTok()
			if err != nil {
				return err
			}

			pred = iri
		}

		if err := t.objectList(subj, pred); err != nil {
			return err
		}

		if !t.acceptP(";") {
			return nil
		}

		for t.acceptP(";") {
		}

		if tok := t.peek(); tok.is(".") || tok.is("]") || tok.is("}") || tok.kind == tEOF {
			return nil
		}
	}
}

func (t *turtle) objectList(subj, pred Term) error {
	for {
		o, err := t.objectTerm()
		if err != nil {
			return err
		}

		g := t.graph

		if t.quads {
			if tok := t.peek(); !tok.is(".") && tok.kind != tEOF {
				if g, err = t.subjectTerm(); err != nil {
					return err
				}
			}
		}

		t.emit(subj, pred, o, g)

		if !t.acceptP(",") {
			return nil
		}
	}
}

func (t *turtle) objectTerm() (Term, error) {
	tok := t.peek()

	switch {
	case tok.kind == tIRI || tok.kind == tPName || tok.kind == tBlank || tok.is("("):
		return t.subjectTerm()
	case tok.is("["):
		if t.peekNext().is("]") {
			t.i += 2

			return t.newBlank(), nil
		}

		t.i++
		b := t.newBlank()

		if err := t.predicateObjectList(b); err != nil {
			return Term{}, err
		}

		return b, t.expectP("]")
	case tok.kw(lexTrue) || tok.kw(lexFalse):
		t.i++

		return Typed(strings.ToLower(tok.val), XSDBoolean), nil
	default:
		return t.parseLiteral()
	}
}
