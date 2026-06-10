package dataplane

import (
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file implements the DAX attribute-projection response sub-protocol, used
// when a Query/Scan/GetItem carries a ProjectionExpression and when an
// UpdateItem requests ReturnValues=UPDATED_OLD/UPDATED_NEW.
//
// Instead of returning items as the usual [key, nonKeyBlob] pairs (or a
// byte-wrapped [attrListId, values...] attribute blob), the client decodes a
// projected item from an ordinal-keyed CBOR map. The ordinals index a list of
// "document paths" the client reconstructs from the ProjectionExpression via
// buildProjectionOrdinals; the server must build the same ordinals here and emit
// {ordinal: leafValue} for each leaf the projection selects from the item.
//
// Two response shapes exist:
//   - Query/Scan/GetItem projection: a bare CBOR map {ordinal: value} per item
//     (decodeProjection on the client side).
//   - UpdateItem UPDATED_*: a byte-wrapped [attrListId, {ordinal: value}] payload
//     (decodeAttributeProjection on the client side), where ordinal indexes the
//     resolved attribute-name list rather than projection document paths.

// documentPathElement is one component of a projected document path: either a
// map attribute name or a list index. index is -1 for a name element.
type documentPathElement struct {
	name  string
	index int
}

func nameElement(name string) documentPathElement { return documentPathElement{name: name, index: -1} }
func indexElement(idx int) documentPathElement    { return documentPathElement{index: idx} }
func (e documentPathElement) isIndex() bool       { return e.index >= 0 }

// documentPath is the ordered element list for one projected term, e.g.
// a.b[2].c decodes to [name a, name b, index 2, name c].
type documentPath struct {
	elements []documentPathElement
}

// projection bundles the projection-expression string handed to the backend
// with the ordinal document paths used to shape the response.
type projection struct {
	expression string
	ordinals   []documentPath
}

// hasProjection reports whether a projection is present.
func (p *projection) hasProjection() bool { return p != nil && len(p.ordinals) > 0 }

// decodeProjectionBlob decodes a ProjectionExpression blob (the
// [version, [path, path, ...]] s-expression form) into a projection: the
// reconstructed expression string (using d's name placeholders) plus the ordered
// document paths the client will key the response by.
func (d *decodedExpression) decodeProjectionBlob(blob []byte) (*projection, error) {
	r := NewReader(newByteReader(blob))

	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	const projectionArity = 2
	if n != projectionArity {
		return nil, errBadExpression
	}

	ver, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	if ver != exprEncodingVersion {
		return nil, errBadExpression
	}

	tree, err := decodeSExpr(r)
	if err != nil {
		return nil, err
	}

	return d.projectionFromTree(tree)
}

// projectionFromTree turns the decoded projection tree (an array of document
// paths) into ordinals plus the rendered ProjectionExpression string.
func (d *decodedExpression) projectionFromTree(tree sexpr) (*projection, error) {
	if tree.children == nil {
		return nil, errBadExpression
	}

	proj := &projection{ordinals: make([]documentPath, 0, len(tree.children))}
	terms := make([]string, 0, len(tree.children))

	for _, pathNode := range tree.children {
		dp, term, err := d.decodeProjectionPath(pathNode)
		if err != nil {
			return nil, err
		}

		proj.ordinals = append(proj.ordinals, dp)
		terms = append(terms, term)
	}

	proj.expression = strings.Join(terms, ", ")

	return proj, nil
}

// decodeProjectionPath decodes one [opDocumentPath, elem, ...] node into a
// documentPath and renders its expression term using #name placeholders.
func (d *decodedExpression) decodeProjectionPath(node sexpr) (documentPath, string, error) {
	code, args, err := funcNode(node)
	if err != nil {
		return documentPath{}, "", err
	}

	if code != opDocumentPath || len(args) == 0 {
		return documentPath{}, "", errBadExpression
	}

	dp := documentPath{elements: make([]documentPathElement, 0, len(args))}

	var b strings.Builder

	for i, elem := range args {
		switch {
		case elem.str != nil:
			dp.elements = append(dp.elements, nameElement(*elem.str))

			if i > 0 {
				b.WriteByte('.')
			}

			b.WriteString(d.nameRef(*elem.str))
		case elem.listIdx != nil:
			dp.elements = append(dp.elements, indexElement(int(*elem.listIdx)))
			b.WriteByte('[')
			b.WriteString(strconv.FormatInt(*elem.listIdx, base10))
			b.WriteByte(']')
		default:
			return documentPath{}, "", errBadExpression
		}
	}

	return dp, b.String(), nil
}

// projectedEntries walks an item along each projection ordinal and returns the
// (ordinal, leafValue) entries the client expects, in ascending-ordinal order.
// Ordinals whose path does not resolve in the item are omitted, matching the
// real service (a projected attribute that is absent is simply not returned).
func projectedEntries(item map[string]types.AttributeValue, ordinals []documentPath) []projectedEntry {
	entries := make([]projectedEntry, 0, len(ordinals))

	for ord, dp := range ordinals {
		if v, ok := resolvePath(item, dp.elements); ok {
			entries = append(entries, projectedEntry{ordinal: ord, value: v})
		}
	}

	return entries
}

// projectedEntry is a single ordinal/value pair in a projected response.
type projectedEntry struct {
	value   types.AttributeValue
	ordinal int
}

// resolvePath walks a top-level item map along the path elements, descending
// into nested maps (name elements) and lists (index elements), and returns the
// leaf value if the full path resolves.
func resolvePath(item map[string]types.AttributeValue, elems []documentPathElement) (types.AttributeValue, bool) {
	if len(elems) == 0 || elems[0].isIndex() {
		return nil, false
	}

	cur, ok := item[elems[0].name]
	if !ok {
		return nil, false
	}

	for _, e := range elems[1:] {
		cur, ok = descend(cur, e)
		if !ok {
			return nil, false
		}
	}

	return cur, true
}

// descend takes one step into an attribute value by the given path element.
func descend(cur types.AttributeValue, e documentPathElement) (types.AttributeValue, bool) {
	if e.isIndex() {
		l, ok := cur.(*types.AttributeValueMemberL)
		if !ok || e.index >= len(l.Value) {
			return nil, false
		}

		return l.Value[e.index], true
	}

	m, ok := cur.(*types.AttributeValueMemberM)
	if !ok {
		return nil, false
	}

	v, ok := m.Value[e.name]

	return v, ok
}

// writeAttributeProjection writes the byte-wrapped [attrListId, {ord: value}]
// payload the client decodes via decodeAttributeProjection for UpdateItem
// UPDATED_OLD/UPDATED_NEW. The ordinals index the sorted attribute-name list
// registered for attrListId, so the client recovers attribute names without the
// server resending them.
func (s *Server) writeAttributeProjection(w *Writer, attrs map[string]types.AttributeValue) error {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}

	id, sorted := s.idForAttrNames(names)

	buf := newByteBuffer()
	inner := NewWriter(buf)

	if err := inner.WriteInt64(id); err != nil {
		return err
	}

	if err := inner.WriteMapHeader(len(sorted)); err != nil {
		return err
	}

	for ord, name := range sorted {
		if err := inner.WriteInt(ord); err != nil {
			return err
		}

		if err := encodeAttributeValue(inner, attrs[name]); err != nil {
			return err
		}
	}

	if err := inner.Flush(); err != nil {
		return err
	}

	return w.WriteBytes(buf.Bytes())
}

// writeProjectionMap writes the bare {ordinal: value} CBOR map a projected
// Query/Scan/GetItem item decodes from (decodeProjection on the client).
func writeProjectionMap(w *Writer, entries []projectedEntry) error {
	if err := w.WriteMapHeader(len(entries)); err != nil {
		return err
	}

	for _, e := range entries {
		if err := w.WriteInt(e.ordinal); err != nil {
			return err
		}

		if err := encodeAttributeValue(w, e.value); err != nil {
			return err
		}
	}

	return nil
}
