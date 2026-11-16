package sparql

import (
	"bytes"
	"encoding/binary"
	"encoding/json/v2"
	"unicode/utf16"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	trixNS = "http://www.w3.org/2004/03/trix/trix-1/"

	maxUint32          = 0xFFFFFFFF
	rdf4jMagic         = "BRDF"
	rdf4jVersion       = 1
	rdf4jRecStatement  = 1
	rdf4jRecEnd        = 127
	rdf4jValNull       = 0
	rdf4jValURI        = 1
	rdf4jValBNode      = 2
	rdf4jValPlain      = 3
	rdf4jValLang       = 4
	rdf4jValDatatype   = 5
	jsonLDType         = "@type"
	jsonLDID           = "@id"
	jsonLDValue        = "@value"
	jsonLDLanguage     = "@language"
	jsonLDGraph        = "@graph"
	jsonLDListKeyword  = "@list"
	jsonLDSetKeyword   = "@set"
	jsonLDContextKey   = "@context"
	jsonLDVocabKey     = "@vocab"
	jsonLDBaseKey      = "@base"
	jsonLDContainerKey = "@container"
)

func groupByGraph(qs []Quad) ([]Term, map[Term][]Quad) {
	var graphs []Term

	by := map[Term][]Quad{}

	for _, q := range qs {
		if _, ok := by[q.G]; !ok {
			graphs = append(graphs, q.G)
		}

		by[q.G] = append(by[q.G], q)
	}

	return graphs, by
}

func writeTriX(buf *bytes.Buffer, qs []Quad) {
	buf.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	buf.WriteString(`<trix xmlns="` + trixNS + `">` + "\n")

	graphs, by := groupByGraph(qs)

	for _, g := range graphs {
		buf.WriteString("  <graph>\n")

		if g.Bound() {
			buf.WriteString("    " + trixTerm(g) + "\n")
		}

		for _, q := range by[g] {
			buf.WriteString("    <triple>\n")
			buf.WriteString("      " + trixTerm(q.S) + "\n")
			buf.WriteString("      " + trixTerm(q.P) + "\n")
			buf.WriteString("      " + trixTerm(q.O) + "\n")
			buf.WriteString("    </triple>\n")
		}

		buf.WriteString("  </graph>\n")
	}

	buf.WriteString("</trix>\n")
}

func trixTerm(t Term) string {
	switch t.Kind {
	case KindIRI:
		return "<uri>" + xmlText(t.Value) + "</uri>"
	case KindBlank:
		return "<id>" + xmlText(t.Value) + "</id>"
	case KindNone, KindLiteral:
	}

	switch {
	case t.Lang != "":
		return `<plainLiteral xml:lang="` + xmlEscape(t.Lang) + `">` + xmlText(t.Value) + "</plainLiteral>"
	case t.Datatype != "":
		return `<typedLiteral datatype="` + xmlEscape(t.Datatype) + `">` + xmlText(t.Value) + "</typedLiteral>"
	}

	return "<plainLiteral>" + xmlText(t.Value) + "</plainLiteral>"
}

func rdf4jString(buf *bytes.Buffer, s string) {
	units := utf16.Encode([]rune(s))
	buf.Write(binary.BigEndian.AppendUint32(nil, uint32(len(units)&maxUint32)))

	for _, u := range units {
		_ = binary.Write(buf, binary.BigEndian, u)
	}
}

func rdf4jValue(buf *bytes.Buffer, t Term) {
	switch t.Kind {
	case KindIRI:
		buf.WriteByte(rdf4jValURI)
		rdf4jString(buf, t.Value)
	case KindBlank:
		buf.WriteByte(rdf4jValBNode)
		rdf4jString(buf, t.Value)
	case KindNone:
		buf.WriteByte(rdf4jValNull)
	case KindLiteral:
		switch {
		case t.Lang != "":
			buf.WriteByte(rdf4jValLang)
			rdf4jString(buf, t.Value)
			rdf4jString(buf, t.Lang)
		case t.Datatype != "":
			buf.WriteByte(rdf4jValDatatype)
			rdf4jString(buf, t.Value)
			rdf4jString(buf, t.Datatype)
		default:
			buf.WriteByte(rdf4jValPlain)
			rdf4jString(buf, t.Value)
		}
	}
}

func writeRDF4JBinary(buf *bytes.Buffer, qs []Quad) {
	buf.WriteString(rdf4jMagic)
	_ = binary.Write(buf, binary.BigEndian, int32(rdf4jVersion))

	for _, q := range qs {
		buf.WriteByte(rdf4jRecStatement)
		rdf4jValue(buf, q.S)
		rdf4jValue(buf, q.P)
		rdf4jValue(buf, q.O)
		rdf4jValue(buf, q.G)
	}

	buf.WriteByte(rdf4jRecEnd)
}

type ldObject = map[string]any

func ldTerm(t Term) ldObject {
	switch {
	case t.Kind == KindIRI:
		return ldObject{jsonLDID: t.Value}
	case t.Kind == KindBlank:
		return ldObject{jsonLDID: "_:" + t.Value}
	case t.Lang != "":
		return ldObject{jsonLDValue: t.Value, jsonLDLanguage: t.Lang}
	case t.Datatype != "":
		return ldObject{jsonLDValue: t.Value, jsonLDType: t.Datatype}
	default:
		return ldObject{jsonLDValue: t.Value}
	}
}

func ldSubjectID(t Term) string {
	if t.Kind == KindBlank {
		return "_:" + t.Value
	}

	return t.Value
}

// ldNodes renders quads as expanded JSON-LD node objects, one per subject in first-seen order.
func ldNodes(qs []Quad) []ldObject {
	var (
		order []Term
		nodes = map[Term]ldObject{}
	)

	for _, q := range qs {
		node, ok := nodes[q.S]
		if !ok {
			node = ldObject{jsonLDID: ldSubjectID(q.S)}
			nodes[q.S] = node

			order = append(order, q.S)
		}

		key, val := q.P.Value, any(ldTerm(q.O))
		if q.P.Value == RDFType && q.O.Kind == KindIRI {
			key, val = jsonLDType, q.O.Value
		}

		list, _ := node[key].([]any)
		node[key] = append(list, val)
	}

	out := make([]ldObject, len(order))
	for i, s := range order {
		out[i] = nodes[s]
	}

	return out
}

func writeJSONLD(buf *bytes.Buffer, qs []Quad) error {
	graphs, by := groupByGraph(qs)

	var doc []ldObject

	for _, g := range graphs {
		nodes := ldNodes(by[g])
		if !g.Bound() {
			doc = append(doc, nodes...)

			continue
		}

		doc = append(doc, ldObject{jsonLDID: ldSubjectID(g), jsonLDGraph: nodes})
	}

	if doc == nil {
		doc = []ldObject{}
	}

	b, err := json.Marshal(doc, jsonx.V1(), json.Deterministic(true))
	if err != nil {
		return err
	}

	buf.Write(b)
	buf.WriteByte('\n')

	return nil
}
