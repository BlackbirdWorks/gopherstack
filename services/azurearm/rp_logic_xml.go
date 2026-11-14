package azurearm

import (
	"encoding/xml"
	"errors"
	"io"
	"maps"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	xmlMaxDepth = 256
	xmlNSURI    = "http://www.w3.org/XML/1998/namespace"
	xmlnsName   = "xmlns"
	xmlPrefix   = "xml"
)

type xmlKind uint8

const (
	xkDoc xmlKind = iota
	xkElem
	xkAttr
	xkText
	xkComment
)

// xmlNode is one node of the XPath data model built from encoding/xml raw tokens, which never read DTDs or
// external entities.
type xmlNode struct {
	ns       map[string]string
	parent   *xmlNode
	space    string
	local    string
	prefix   string
	value    string
	children []*xmlNode
	attrs    []*xmlNode
	all      []*xmlNode
	order    int
	end      int
	kind     xmlKind
}

type xmlBuilder struct {
	doc   *xmlNode
	cur   *xmlNode
	depth int
	roots int
}

func parseLogicXML(s string) (*xmlNode, error) {
	dec := xml.NewDecoder(strings.NewReader(s))
	doc := &xmlNode{kind: xkDoc}
	b := &xmlBuilder{doc: doc, cur: doc}

	for {
		tok, err := dec.RawToken()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, exprErr("the value is not valid XML: %v", err)
		}

		if terr := b.token(tok); terr != nil {
			return nil, terr
		}
	}

	if b.cur != doc || b.roots != 1 {
		return nil, exprErr("the value is not valid XML: it must contain exactly one complete root element")
	}

	doc.number(doc)

	return doc, nil
}

func (b *xmlBuilder) token(tok xml.Token) error {
	switch t := tok.(type) {
	case xml.StartElement:
		return b.start(t)
	case xml.EndElement:
		return b.endElem(t)
	case xml.CharData:
		return b.text(string(t))
	case xml.Comment:
		if b.cur != b.doc {
			b.add(&xmlNode{kind: xkComment, value: string(t)})
		}
	case xml.Directive:
		return exprErr("the value is not valid XML: DTD declarations are not allowed")
	case xml.ProcInst:
	}

	return nil
}

func (b *xmlBuilder) add(n *xmlNode) {
	n.parent = b.cur
	b.cur.children = append(b.cur.children, n)
}

func (b *xmlBuilder) text(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	if b.cur == b.doc {
		return exprErr("the value is not valid XML: data at the root level is invalid")
	}

	b.add(&xmlNode{kind: xkText, value: s})

	return nil
}

func (b *xmlBuilder) start(t xml.StartElement) error {
	if b.cur == b.doc {
		b.roots++
		if b.roots > 1 {
			return exprErr("the value is not valid XML: multiple root elements")
		}
	}

	b.depth++
	if b.depth > xmlMaxDepth {
		return exprErr("the XML nesting is deeper than %d levels", xmlMaxDepth)
	}

	n := &xmlNode{kind: xkElem, local: t.Name.Local, prefix: t.Name.Space}
	b.add(n)
	b.cur = n

	var plain []xml.Attr

	for _, a := range t.Attr {
		switch {
		case a.Name.Space == "" && a.Name.Local == xmlnsName:
			n.declare("", a.Value)
		case a.Name.Space == xmlnsName:
			n.declare(a.Name.Local, a.Value)
		default:
			plain = append(plain, a)
		}
	}

	space, err := n.resolve(n.prefix, true)
	if err != nil {
		return err
	}

	n.space = space

	for _, a := range plain {
		an := &xmlNode{kind: xkAttr, parent: n, local: a.Name.Local, prefix: a.Name.Space, value: a.Value}

		if a.Name.Space != "" {
			if an.space, err = n.resolve(a.Name.Space, false); err != nil {
				return err
			}
		}

		n.attrs = append(n.attrs, an)
	}

	return nil
}

func (b *xmlBuilder) endElem(t xml.EndElement) error {
	if b.cur == b.doc || b.cur.local != t.Name.Local || b.cur.prefix != t.Name.Space {
		return exprErr("the value is not valid XML: mismatched end tag '%s'", t.Name.Local)
	}

	b.cur = b.cur.parent
	b.depth--

	return nil
}

func (n *xmlNode) declare(prefix, uri string) {
	if n.ns == nil {
		n.ns = map[string]string{}
	}

	n.ns[prefix] = uri
}

func (n *xmlNode) resolve(prefix string, isElem bool) (string, error) {
	if prefix == xmlPrefix {
		return xmlNSURI, nil
	}

	for e := n; e != nil; e = e.parent {
		if uri, ok := e.ns[prefix]; ok {
			return uri, nil
		}
	}

	if prefix == "" && isElem {
		return "", nil
	}

	return "", exprErr("the value is not valid XML: the prefix '%s' is not declared", prefix)
}

// number assigns document order (element, its attributes, then its children) and subtree end markers.
func (n *xmlNode) number(root *xmlNode) {
	n.order = len(root.all)
	root.all = append(root.all, n)

	for _, a := range n.attrs {
		a.order = len(root.all)
		a.end = a.order
		root.all = append(root.all, a)
	}

	for _, c := range n.children {
		c.number(root)
	}

	n.end = len(root.all) - 1
}

func (n *xmlNode) root() *xmlNode {
	r := n
	for r.parent != nil {
		r = r.parent
	}

	return r
}

func (n *xmlNode) qname() string {
	if n.prefix == "" {
		return n.local
	}

	return n.prefix + ":" + n.local
}

func (n *xmlNode) stringValue() string {
	switch n.kind {
	case xkText, xkAttr, xkComment:
		return n.value
	case xkDoc, xkElem:
	}

	var sb strings.Builder

	n.collectText(&sb)

	return sb.String()
}

func (n *xmlNode) collectText(sb *strings.Builder) {
	for _, c := range n.children {
		switch c.kind {
		case xkText:
			sb.WriteString(c.value)
		case xkElem:
			c.collectText(sb)
		case xkDoc, xkAttr, xkComment:
		}
	}
}

var (
	xmlTextEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")    //nolint:gochecknoglobals // static
	xmlAttrEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", "\"", "&quot;") //nolint:gochecknoglobals // static
)

// serialize renders a node as XML, declaring any namespace the output scope does not yet know.
func (n *xmlNode) serialize() string {
	var sb strings.Builder

	n.write(&sb, map[string]string{})

	return sb.String()
}

func (n *xmlNode) write(sb *strings.Builder, scope map[string]string) {
	switch n.kind {
	case xkDoc:
		for _, c := range n.children {
			c.write(sb, scope)
		}
	case xkText:
		sb.WriteString(xmlTextEscaper.Replace(n.value))
	case xkComment:
		sb.WriteString("<!--" + n.value + "-->")
	case xkAttr:
		sb.WriteString(xmlAttrEscaper.Replace(n.value))
	case xkElem:
		n.writeElem(sb, scope)
	}
}

func (n *xmlNode) writeElem(sb *strings.Builder, scope map[string]string) {
	inner := maps.Clone(scope)

	sb.WriteString("<" + n.qname())

	need := make([]*xmlNode, 0, 1+len(n.attrs))
	need = append(need, n)
	need = append(need, n.attrs...)

	for _, x := range need {
		if x.prefix == "" && x.kind == xkAttr {
			continue
		}

		if cur, has := inner[x.prefix]; (!has && x.space != "") || (has && cur != x.space) {
			inner[x.prefix] = x.space

			if x.prefix == "" {
				sb.WriteString(` xmlns="` + xmlAttrEscaper.Replace(x.space) + `"`)
			} else {
				sb.WriteString(" xmlns:" + x.prefix + `="` + xmlAttrEscaper.Replace(x.space) + `"`)
			}
		}
	}

	for _, a := range n.attrs {
		sb.WriteString(" " + a.qname() + `="` + xmlAttrEscaper.Replace(a.value) + `"`)
	}

	if len(n.children) == 0 {
		sb.WriteString("/>")

		return
	}

	sb.WriteString(">")

	for _, c := range n.children {
		c.write(sb, inner)
	}

	sb.WriteString("</" + n.qname() + ">")
}

// logicXML implements xml(): a string must hold well-formed XML; an object must have one non-array root property.
func logicXML(a []any) (any, error) {
	switch v := a[0].(type) {
	case string:
		if _, err := parseLogicXML(v); err != nil {
			return nil, err
		}

		return v, nil
	case map[string]any:
		out, err := logicJSONToXML(v)
		if err != nil {
			return nil, err
		}

		return out, nil
	}

	return nil, exprErr("the xml function expects a string or an object, not %s", logicTypeName(a[0]))
}

func logicTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case []any:
		return "an array"
	case bool:
		return "a boolean"
	case int64, float64:
		return "a number"
	}

	return "this value"
}

func logicJSONToXML(obj map[string]any) (string, error) {
	if len(obj) != 1 {
		return "", exprErr("the JSON object passed to xml must have exactly one root property")
	}

	var sb strings.Builder

	for k, v := range obj {
		if _, isArr := v.([]any); isArr {
			return "", exprErr("the root property of the JSON object passed to xml cannot be an array")
		}

		if err := writeJSONElem(&sb, k, v, 0); err != nil {
			return "", err
		}
	}

	if _, err := parseLogicXML(sb.String()); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func validXMLName(s string) bool {
	if s == "" {
		return false
	}

	for i, r := range s {
		ok := unicode.IsLetter(r) || r == '_' || (i > 0 && (unicode.IsDigit(r) || r == '-' || r == '.'))
		if !ok {
			return false
		}
	}

	return true
}

func jsonScalarText(v any) string {
	switch t := v.(type) {
	case bool:
		return strconv.FormatBool(t)
	case string:
		return t
	}

	return logicString(v)
}

func writeJSONElem(sb *strings.Builder, name string, v any, depth int) error {
	if depth > xmlMaxDepth {
		return exprErr("the JSON object passed to xml is nested deeper than %d levels", xmlMaxDepth)
	}

	if !validXMLName(name) {
		return exprErr("the property name '%s' is not a valid XML element name", name)
	}

	switch t := v.(type) {
	case []any:
		for _, it := range t {
			if err := writeJSONElem(sb, name, it, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		return writeJSONObject(sb, name, t, depth)
	case nil:
		sb.WriteString("<" + name + "/>")
	default:
		sb.WriteString("<" + name + ">" + xmlTextEscaper.Replace(jsonScalarText(v)) + "</" + name + ">")
	}

	return nil
}

func writeJSONObject(sb *strings.Builder, name string, obj map[string]any, depth int) error {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	sb.WriteString("<" + name)

	for _, k := range keys {
		if attr, ok := strings.CutPrefix(k, "@"); ok {
			if !validXMLName(attr) {
				return exprErr("the property name '%s' is not a valid XML attribute name", k)
			}

			sb.WriteString(" " + attr + `="` + xmlAttrEscaper.Replace(jsonScalarText(obj[k])) + `"`)
		}
	}

	sb.WriteString(">")

	if txt, has := obj["#text"]; has {
		sb.WriteString(xmlTextEscaper.Replace(jsonScalarText(txt)))
	}

	for _, k := range keys {
		if k == "#text" || strings.HasPrefix(k, "@") {
			continue
		}

		if err := writeJSONElem(sb, k, obj[k], depth+1); err != nil {
			return err
		}
	}

	sb.WriteString("</" + name + ">")

	return nil
}

// logicXMLDoc turns an xpath() first argument into a parsed document.
func logicXMLDoc(v any) (*xmlNode, error) {
	switch t := v.(type) {
	case string:
		return parseLogicXML(t)
	case map[string]any:
		s, err := logicJSONToXML(t)
		if err != nil {
			return nil, err
		}

		return parseLogicXML(s)
	}

	return nil, exprErr("the xpath function expects XML as its first argument")
}
