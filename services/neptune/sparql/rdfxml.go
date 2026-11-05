package sparql

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
)

const xmlNS = "http://www.w3.org/XML/1998/namespace"

type xmlParser struct {
	dec   *xml.Decoder
	doc   string
	base  string
	graph Term
	out   []Quad
	li    []int
	opts  ParseOptions
	bn    int
}

func parseRDFXML(data []byte, opts ParseOptions, doc string) ([]Quad, error) {
	p := &xmlParser{
		dec:   xml.NewDecoder(bytes.NewReader(data)),
		opts:  opts,
		doc:   doc,
		graph: opts.graph(),
		base:  opts.base(),
	}
	p.dec.Strict = true

	for {
		tok, err := p.dec.Token()
		if errors.Is(err, io.EOF) {
			return p.out, nil
		}

		if err != nil {
			return p.out, &ParseError{Msg: err.Error()}
		}

		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		if start.Name.Space == NSRDF && start.Name.Local == "RDF" {
			lang, restore := p.scope(start.Attr, "")
			defer restore()

			return p.out, p.rdfRoot(lang)
		}

		_, err = p.nodeElement(start, "")

		return p.out, err
	}
}

func (p *xmlParser) fail(msg string) error {
	line, _ := p.dec.InputPos()

	return &ParseError{Msg: msg, Record: line}
}

func (p *xmlParser) rdfRoot(lang string) error {
	for {
		tok, err := p.dec.Token()
		if err != nil {
			return &ParseError{Msg: err.Error()}
		}

		switch t := tok.(type) {
		case xml.StartElement:
			if _, err = p.nodeElement(t, lang); err != nil {
				return err
			}
		case xml.EndElement:
			return nil
		}
	}
}

func (p *xmlParser) newBlank() Term {
	p.bn++

	return Blank(p.doc + "x" + strconv.Itoa(p.bn))
}

func (p *xmlParser) resolve(ref string) string {
	b, err := url.Parse(p.base)
	if err != nil {
		return ref
	}

	r, err := url.Parse(ref)
	if err != nil {
		return ref
	}

	return b.ResolveReference(r).String()
}

func isXMLAttr(a xml.Attr, local string) bool {
	return (a.Name.Space == xmlNS || a.Name.Space == "xml") && a.Name.Local == local
}

func isRDFAttr(a xml.Attr, local string) bool {
	return a.Name.Space == NSRDF && a.Name.Local == local
}

func (p *xmlParser) emit(s, pr, o Term) {
	p.out = append(p.out, Quad{S: s, P: pr, O: o, G: p.graph})
}

func (p *xmlParser) scope(attrs []xml.Attr, lang string) (string, func()) {
	prev := p.base

	for _, a := range attrs {
		if isXMLAttr(a, "base") {
			p.base = p.resolve(a.Value)
		}

		if isXMLAttr(a, "lang") {
			lang = a.Value
		}
	}

	return lang, func() { p.base = prev }
}

func (p *xmlParser) subjectOf(attrs []xml.Attr) Term {
	for _, a := range attrs {
		switch {
		case isRDFAttr(a, "about"):
			return IRI(p.resolve(a.Value))
		case isRDFAttr(a, "ID"):
			return IRI(p.resolve("#" + a.Value))
		case isRDFAttr(a, "nodeID"):
			return Blank(p.doc + a.Value)
		}
	}

	return p.newBlank()
}

func (p *xmlParser) nodeElement(start xml.StartElement, lang string) (Term, error) {
	lang, restore := p.scope(start.Attr, lang)
	defer restore()

	subj := p.subjectOf(start.Attr)
	p.li = append(p.li, 0)

	defer func() { p.li = p.li[:len(p.li)-1] }()

	if start.Name.Space != NSRDF || start.Name.Local != "Description" {
		p.emit(subj, IRI(RDFType), IRI(start.Name.Space+start.Name.Local))
	}

	p.propertyAttrs(start, subj, lang)

	for {
		tok, err := p.dec.Token()
		if err != nil {
			return Term{}, &ParseError{Msg: err.Error()}
		}

		switch t := tok.(type) {
		case xml.StartElement:
			if err = p.propertyElement(t, subj, lang); err != nil {
				return Term{}, err
			}
		case xml.EndElement:
			return subj, nil
		}
	}
}

// propertyAttrs emits the property attributes of a node element as literal (or rdf:type) statements.
func (p *xmlParser) propertyAttrs(start xml.StartElement, subj Term, lang string) {
	for _, a := range start.Attr {
		switch {
		case a.Name.Space == "xmlns" || a.Name.Space == xmlNS || a.Name.Space == "xml" || a.Name.Space == "":
		case a.Name.Space == NSRDF && (a.Name.Local == "about" || a.Name.Local == "ID" || a.Name.Local == "nodeID"):
		case a.Name.Space == NSRDF && a.Name.Local == "type":
			p.emit(subj, IRI(RDFType), IRI(p.resolve(a.Value)))
		default:
			p.emit(subj, IRI(a.Name.Space+a.Name.Local), p.plain(a.Value, lang))
		}
	}
}

func (p *xmlParser) plain(v, lang string) Term {
	if lang != "" {
		return LangLiteral(v, lang)
	}

	return Literal(v)
}

func (p *xmlParser) predicateOf(start xml.StartElement) Term {
	if start.Name.Space == NSRDF && start.Name.Local == "li" {
		p.li[len(p.li)-1]++

		return IRI(NSRDF + "_" + strconv.Itoa(p.li[len(p.li)-1]))
	}

	return IRI(start.Name.Space + start.Name.Local)
}

type propAttrs struct {
	parseType, datatype string
	resource, nodeID    string
	hasResource         bool
	hasNodeID           bool
}

func readPropAttrs(attrs []xml.Attr) propAttrs {
	var pa propAttrs

	for _, a := range attrs {
		switch {
		case isRDFAttr(a, "parseType"):
			pa.parseType = a.Value
		case isRDFAttr(a, "datatype"):
			pa.datatype = a.Value
		case isRDFAttr(a, "resource"):
			pa.resource, pa.hasResource = a.Value, true
		case isRDFAttr(a, "nodeID"):
			pa.nodeID, pa.hasNodeID = a.Value, true
		}
	}

	return pa
}

func (p *xmlParser) propertyElement(start xml.StartElement, subj Term, lang string) error {
	lang, restore := p.scope(start.Attr, lang)
	defer restore()

	pred := p.predicateOf(start)
	pa := readPropAttrs(start.Attr)

	switch {
	case pa.parseType == "Resource":
		obj := p.newBlank()
		p.emit(subj, pred, obj)

		return p.resourceBody(obj, lang)
	case pa.parseType == "Collection":
		return p.collectionBody(subj, pred, lang)
	case pa.parseType != "":
		return p.fail("rdf:parseType=" + strconv.Quote(pa.parseType) + " is not supported")
	case pa.hasResource || pa.hasNodeID:
		obj := Blank(p.doc + pa.nodeID)
		if pa.hasResource {
			obj = IRI(p.resolve(pa.resource))
		}

		p.emit(subj, pred, obj)

		return p.skipToEnd()
	}

	return p.contentBody(subj, pred, pa, lang)
}

func (p *xmlParser) skipToEnd() error {
	depth := 1

	for depth > 0 {
		tok, err := p.dec.Token()
		if err != nil {
			return &ParseError{Msg: err.Error()}
		}

		switch tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}

	return nil
}

func (p *xmlParser) resourceBody(obj Term, lang string) error {
	for {
		tok, err := p.dec.Token()
		if err != nil {
			return &ParseError{Msg: err.Error()}
		}

		switch t := tok.(type) {
		case xml.StartElement:
			if err = p.propertyElement(t, obj, lang); err != nil {
				return err
			}
		case xml.EndElement:
			return nil
		}
	}
}

func (p *xmlParser) collectionBody(subj, pred Term, lang string) error {
	var items []Term

	for {
		tok, err := p.dec.Token()
		if err != nil {
			return &ParseError{Msg: err.Error()}
		}

		switch t := tok.(type) {
		case xml.StartElement:
			n, nerr := p.nodeElement(t, lang)
			if nerr != nil {
				return nerr
			}

			items = append(items, n)
		case xml.EndElement:
			p.emitList(subj, pred, items)

			return nil
		}
	}
}

func (p *xmlParser) emitList(subj, pred Term, items []Term) {
	if len(items) == 0 {
		p.emit(subj, pred, IRI(RDFNil))

		return
	}

	cells := make([]Term, len(items))
	for i := range cells {
		cells[i] = p.newBlank()
	}

	p.emit(subj, pred, cells[0])

	for i, it := range items {
		p.emit(cells[i], IRI(RDFFirst), it)

		if i == len(items)-1 {
			p.emit(cells[i], IRI(RDFRest), IRI(RDFNil))
		} else {
			p.emit(cells[i], IRI(RDFRest), cells[i+1])
		}
	}
}

func (p *xmlParser) contentBody(subj, pred Term, pa propAttrs, lang string) error {
	var text strings.Builder

	for {
		tok, err := p.dec.Token()
		if err != nil {
			return &ParseError{Msg: err.Error()}
		}

		switch t := tok.(type) {
		case xml.CharData:
			text.Write(t)
		case xml.StartElement:
			if strings.TrimSpace(text.String()) != "" {
				return p.fail("mixed content in property element")
			}

			obj, nerr := p.nodeElement(t, lang)
			if nerr != nil {
				return nerr
			}

			p.emit(subj, pred, obj)

			return p.skipToEnd()
		case xml.EndElement:
			if pa.datatype != "" {
				p.emit(subj, pred, Typed(text.String(), p.resolve(pa.datatype)))
			} else {
				p.emit(subj, pred, p.plain(text.String(), lang))
			}

			return nil
		}
	}
}
