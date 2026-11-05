// Package sparql is an in-process RDF quad store with a SPARQL 1.1 query and update engine.
package sparql

import (
	"strconv"
	"strings"
)

// Kind classifies an RDF term.
type Kind uint8

// Term kinds; KindNone is the zero value and means unbound.
const (
	KindNone Kind = iota
	KindIRI
	KindBlank
	KindLiteral
)

// Well-known IRIs.
const (
	NSXSD         = "http://www.w3.org/2001/XMLSchema#"
	NSRDF         = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	NSRDFS        = "http://www.w3.org/2000/01/rdf-schema#"
	NSOWL         = "http://www.w3.org/2002/07/owl#"
	XSDString     = NSXSD + "string"
	XSDInteger    = NSXSD + "integer"
	XSDDecimal    = NSXSD + "decimal"
	XSDDouble     = NSXSD + "double"
	XSDFloat      = NSXSD + "float"
	XSDBoolean    = NSXSD + "boolean"
	XSDDateTime   = NSXSD + "dateTime"
	RDFType       = NSRDF + "type"
	RDFFirst      = NSRDF + "first"
	RDFRest       = NSRDF + "rest"
	RDFNil        = NSRDF + "nil"
	RDFLangString = NSRDF + "langString"
	lexTrue       = "true"
	lexFalse      = "false"
	// DefaultGraph is the named graph Neptune uses when no graph is given.
	DefaultGraph = "http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph"
	// DefaultBase is the base IRI Neptune applies when a request carries none.
	DefaultBase = "http://aws.amazon.com/neptune/default/"
)

// Term is an RDF term. A plain literal has an empty Datatype; a language-tagged one has Lang set.
type Term struct {
	Value    string
	Datatype string
	Lang     string
	Kind     Kind
}

// IRI returns an IRI term.
func IRI(s string) Term { return Term{Kind: KindIRI, Value: s} }

// Blank returns a blank-node term.
func Blank(s string) Term { return Term{Kind: KindBlank, Value: s} }

// Literal returns a plain (xsd:string) literal.
func Literal(s string) Term { return Term{Kind: KindLiteral, Value: s} }

// LangLiteral returns a language-tagged literal.
func LangLiteral(s, lang string) Term {
	return Term{Kind: KindLiteral, Value: s, Lang: strings.ToLower(lang)}
}

// Typed returns a datatyped literal; xsd:string collapses to a plain literal.
func Typed(s, dt string) Term {
	if dt == XSDString {
		dt = ""
	}

	return Term{Kind: KindLiteral, Value: s, Datatype: dt}
}

// Bound reports whether the term is set.
func (t Term) Bound() bool { return t.Kind != KindNone }

// IsIRI reports whether t is an IRI.
func (t Term) IsIRI() bool { return t.Kind == KindIRI }

// DT returns the effective datatype IRI of a literal.
func (t Term) DT() string {
	switch {
	case t.Lang != "":
		return RDFLangString
	case t.Datatype == "":
		return XSDString
	default:
		return t.Datatype
	}
}

// String renders the term in N-Triples syntax.
func (t Term) String() string {
	switch t.Kind {
	case KindIRI:
		return "<" + escapeIRI(t.Value) + ">"
	case KindBlank:
		return "_:" + t.Value
	case KindLiteral:
		s := `"` + escapeString(t.Value) + `"`
		switch {
		case t.Lang != "":
			return s + "@" + t.Lang
		case t.Datatype != "":
			return s + "^^<" + escapeIRI(t.Datatype) + ">"
		}

		return s
	default:
		return ""
	}
}

func escapeString(s string) string {
	var sb strings.Builder

	for _, r := range s {
		switch r {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

const hexWidth = 4

func escapeIRI(s string) string {
	var sb strings.Builder

	for _, r := range s {
		if r <= ' ' || strings.ContainsRune(`<>"{}|^`+"`"+`\`, r) {
			sb.WriteString(`\u` + strings.ToUpper(leftPad(strconv.FormatInt(int64(r), 16), hexWidth)))

			continue
		}

		sb.WriteRune(r)
	}

	return sb.String()
}

func leftPad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}

	return s
}

// Triple is a subject-predicate-object statement.
type Triple struct{ S, P, O Term }

// Quad is a triple in a named graph.
type Quad struct {
	Triple
	G Term
}
