package sparql

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

func writeQuad(buf *bytes.Buffer, q Quad, withGraph bool) {
	buf.WriteString(q.S.String() + " " + q.P.String() + " " + q.O.String())

	if withGraph && q.G.Bound() {
		buf.WriteString(" " + q.G.String())
	}

	buf.WriteString(" .\n")
}

// SerializeQuads renders quads in the given format. Triple-only formats ignore graph names.
func SerializeQuads(f Format, qs []Quad) ([]byte, error) {
	var buf bytes.Buffer

	switch f {
	case FormatNTriples, FormatNQuads:
		for _, q := range qs {
			writeQuad(&buf, q, f == FormatNQuads)
		}
	case FormatTurtle:
		writeTurtle(&buf, qs, "")
	case FormatTriG:
		writeTriG(&buf, qs)
	case FormatRDFXML:
		if err := writeRDFXML(&buf, qs); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// SerializeTriples renders triples (no graph names).
func SerializeTriples(f Format, ts []Triple) ([]byte, error) {
	qs := make([]Quad, len(ts))
	for i, t := range ts {
		qs[i] = Quad{Triple: t}
	}

	return SerializeQuads(f, qs)
}

func writeTurtle(buf *bytes.Buffer, qs []Quad, indent string) {
	var subjects []Term

	by := map[Term][]Quad{}

	for _, q := range qs {
		if _, ok := by[q.S]; !ok {
			subjects = append(subjects, q.S)
		}

		by[q.S] = append(by[q.S], q)
	}

	for _, s := range subjects {
		buf.WriteString(indent + s.String())

		for i, q := range by[s] {
			sep := " ;\n" + indent + "    "
			if i == 0 {
				sep = " "
			}

			p := q.P.String()
			if q.P.Value == RDFType {
				p = "a"
			}

			buf.WriteString(sep + p + " " + q.O.String())
		}

		buf.WriteString(" .\n")
	}
}

func writeTriG(buf *bytes.Buffer, qs []Quad) {
	var graphs []Term

	by := map[Term][]Quad{}

	for _, q := range qs {
		if _, ok := by[q.G]; !ok {
			graphs = append(graphs, q.G)
		}

		by[q.G] = append(by[q.G], q)
	}

	for _, g := range graphs {
		if !g.Bound() {
			writeTurtle(buf, by[g], "")

			continue
		}

		buf.WriteString(g.String() + " {\n")
		writeTurtle(buf, by[g], "  ")
		buf.WriteString("}\n")
	}
}

func splitQName(iri string) (string, string, bool) {
	for i := len(iri) - 1; i > 0; i-- {
		r := rune(iri[i])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.') {
			ns, local := iri[:i+1], iri[i+1:]
			if local != "" && (unicode.IsLetter(rune(local[0])) || local[0] == '_') {
				return ns, local, true
			}

			break
		}
	}

	return "", "", false
}

func writeRDFXML(buf *bytes.Buffer, qs []Quad) error {
	buf.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	buf.WriteString(`<rdf:RDF xmlns:rdf="` + NSRDF + `">` + "\n")

	var subjects []Term

	by := map[Term][]Quad{}

	for _, q := range qs {
		if _, ok := by[q.S]; !ok {
			subjects = append(subjects, q.S)
		}

		by[q.S] = append(by[q.S], q)
	}

	for _, s := range subjects {
		attr := "rdf:about"
		if s.Kind == KindBlank {
			attr = "rdf:nodeID"
		}

		buf.WriteString("  <rdf:Description " + attr + `="` + xmlEscape(s.Value) + `">` + "\n")

		for _, q := range by[s] {
			if err := writeXMLProperty(buf, q); err != nil {
				return err
			}
		}

		buf.WriteString("  </rdf:Description>\n")
	}

	buf.WriteString("</rdf:RDF>\n")

	return nil
}

func xmlEscape(s string) string {
	var b bytes.Buffer

	_ = xml.EscapeText(&b, []byte(s))

	return strings.ReplaceAll(b.String(), `"`, "&#34;")
}

func writeXMLProperty(buf *bytes.Buffer, q Quad) error {
	ns, local, ok := splitQName(q.P.Value)
	if !ok {
		return fmt.Errorf("predicate %s cannot be written as RDF/XML", q.P.Value) //nolint:err113 // reported to the caller verbatim
	}

	open := "    <p:" + local + ` xmlns:p="` + xmlEscape(ns) + `"`

	switch q.O.Kind {
	case KindIRI:
		buf.WriteString(open + ` rdf:resource="` + xmlEscape(q.O.Value) + `"/>` + "\n")
	case KindBlank:
		buf.WriteString(open + ` rdf:nodeID="` + xmlEscape(q.O.Value) + `"/>` + "\n")
	default:
		extra := ""

		switch {
		case q.O.Lang != "":
			extra = ` xml:lang="` + xmlEscape(q.O.Lang) + `"`
		case q.O.Datatype != "":
			extra = ` rdf:datatype="` + xmlEscape(q.O.Datatype) + `"`
		}

		buf.WriteString(open + extra + ">" + xmlEscape(q.O.Value) + "</p:" + local + ">\n")
	}

	return nil
}

// SortedGraphNames renders graph IRIs for listings.
func SortedGraphNames(gs []Term) []string {
	out := make([]string, len(gs))
	for i, g := range gs {
		out[i] = g.Value
	}

	sort.Strings(out)

	return out
}
