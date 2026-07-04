package dataplane

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file decodes the DAX pre-parsed expression sub-protocol back into a
// DynamoDB expression string plus ExpressionAttributeNames / Values, so that
// UpdateItem / Query / Scan can delegate to the gopherstack DynamoDB backend
// (which parses real expression strings).
//
// The DAX client parses UpdateExpression / ConditionExpression /
// KeyConditionExpression / FilterExpression / ProjectionExpression on the client
// side into a CBOR s-expression tree. The encoder is
// github.com/aws/aws-dax-go/dax/internal/parser.ExpressionEncoder; this is the
// inverse.
//
// Wire shape of one encoded expression blob:
//
//	projection: [version, tree]
//	otherwise:  [version, tree, [variableValue, ...]]
//
// where tree is a nested array/atom structure. An atom is a CBOR string (a
// document-path element), a CBOR int (a function code or variable id), or a
// tag-3324 int (a list-access index). A compound node is a CBOR array whose
// first element is a function-code atom followed by its operands.

const exprEncodingVersion = 1

// tagDocumentPathOrdinal tags a list-access index inside a document path.
const tagDocumentPathOrdinal = 3324

// Expression operator codes, matching parser/expression.go in aws-dax-go.
const (
	opEqual = iota
	opNotEqual
	opLessThan
	opGreaterEqual
	opGreaterThan
	opLessEqual

	opAnd
	opOr
	opNot

	opBetween

	opIn

	opAttributeExists
	opAttributeNotExists
	opAttributeType
	opBeginsWith
	opContains
	opSize

	opVariable
	opDocumentPath

	opSetAction
	opAddAction
	opDeleteAction
	opRemoveAction

	opIfNotExists
	opListAppend
	opPlus
	opMinus
)

var errBadExpression = errors.New("dax: malformed encoded expression")

// errProjectionUnsupported is returned when a ProjectionExpression is present on
// a Query/Scan, whose projected response sub-protocol is not served. The base
// (non-projected) item path is fully supported.
var errProjectionUnsupported = errors.New(
	"dax: ProjectionExpression on Query/Scan is not supported by the data plane",
)

// decodedExpression is the reconstructed DynamoDB-side form of one or more DAX
// expression blobs decoded together so they share one name/value namespace.
type decodedExpression struct {
	names  map[string]string
	byName map[string]string // reverse map: attribute name -> placeholder
	values map[string]types.AttributeValue
	nextN  int
	nextV  int
}

func newDecodedExpression() *decodedExpression {
	return &decodedExpression{
		names:  map[string]string{},
		byName: map[string]string{},
		values: map[string]types.AttributeValue{},
	}
}

// nameRef registers an attribute name as a #placeholder and returns the
// placeholder token. Reusing placeholders for repeated names keeps the output
// compact and avoids reserved-word collisions, mirroring how the SDK builds
// expressions.
func (d *decodedExpression) nameRef(name string) string {
	if ph, ok := d.byName[name]; ok {
		return ph
	}

	ph := fmt.Sprintf("#n%d", d.nextN)
	d.nextN++
	d.names[ph] = name
	d.byName[name] = ph

	return ph
}

// valueRef registers a value as a :placeholder and returns the placeholder
// token.
func (d *decodedExpression) valueRef(v types.AttributeValue) string {
	ph := fmt.Sprintf(":v%d", d.nextV)
	d.nextV++
	d.values[ph] = v

	return ph
}

// names/values getters return nil when empty so callers can leave the SDK input
// fields unset.
func (d *decodedExpression) namesMap() map[string]string {
	if len(d.names) == 0 {
		return nil
	}

	return d.names
}

func (d *decodedExpression) valuesMap() map[string]types.AttributeValue {
	if len(d.values) == 0 {
		return nil
	}

	return d.values
}

// readExpressionBlob reads one byte-wrapped encoded expression and returns its
// reconstructed string for the given expression type, recording any attribute
// names/values it introduces into d. exprType is one of the parser expression
// kinds and controls update-action vs condition rendering.
func (d *decodedExpression) readExpressionBlob(r *Reader, exprType exprKind) (string, error) {
	blob, err := r.ReadBytes()
	if err != nil {
		return "", err
	}

	return d.decodeBlob(blob, exprType)
}

// exprKind distinguishes how a decoded expression tree is rendered.
type exprKind int

const (
	kindUpdate exprKind = iota
	kindCondition
	kindProjection
)

func (d *decodedExpression) decodeBlob(blob []byte, exprType exprKind) (string, error) {
	r := NewReader(bytes.NewReader(blob))

	n, err := r.ReadArrayLength()
	if err != nil {
		return "", err
	}

	const projectionArity, fullArity = 2, 3
	if n != projectionArity && n != fullArity {
		return "", errBadExpression
	}

	ver, err := r.ReadInt()
	if err != nil {
		return "", err
	}

	if ver != exprEncodingVersion {
		return "", errBadExpression
	}

	tree, err := decodeSExpr(r)
	if err != nil {
		return "", err
	}

	var vals []types.AttributeValue

	if n == fullArity {
		if vals, err = decodeVariableValues(r); err != nil {
			return "", err
		}
	}

	return d.render(tree, vals, exprType)
}

func decodeVariableValues(r *Reader) ([]types.AttributeValue, error) {
	count, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	vals := make([]types.AttributeValue, count)

	for i := range count {
		if vals[i], err = decodeAttributeValue(r); err != nil {
			return nil, err
		}
	}

	return vals, nil
}

// sexpr is a decoded s-expression node: exactly one of the fields is set.
type sexpr struct {
	str      *string // document-path name element
	num      *int64  // function code or variable id (positive int atom)
	listIdx  *int64  // tag-3324 list-access index
	children []sexpr // compound node
}

func decodeSExpr(r *Reader) (sexpr, error) {
	hdr, err := r.PeekHeader()
	if err != nil {
		return sexpr{}, err
	}

	switch int(hdr) & majorTypeMask {
	case majorArray:
		return decodeSExprArray(r)
	case majorUtf:
		s, e := r.ReadString()

		return sexpr{str: &s}, e
	case majorPosInt, majorNegInt:
		v, e := r.ReadInt64()

		return sexpr{num: &v}, e
	case majorTag:
		return decodeSExprTag(r)
	default:
		return sexpr{}, errBadExpression
	}
}

func decodeSExprArray(r *Reader) (sexpr, error) {
	count, err := r.ReadArrayLength()
	if err != nil {
		return sexpr{}, err
	}

	children := make([]sexpr, count)

	for i := range count {
		if children[i], err = decodeSExpr(r); err != nil {
			return sexpr{}, err
		}
	}

	return sexpr{children: children}, nil
}

func decodeSExprTag(r *Reader) (sexpr, error) {
	_, tag, err := r.readHeader()
	if err != nil {
		return sexpr{}, err
	}

	if tag != tagDocumentPathOrdinal {
		return sexpr{}, errBadExpression
	}

	idx, err := r.ReadInt64()
	if err != nil {
		return sexpr{}, err
	}

	return sexpr{listIdx: &idx}, nil
}

// render turns a decoded tree into a DynamoDB expression fragment.
func (d *decodedExpression) render(tree sexpr, vals []types.AttributeValue, exprType exprKind) (string, error) {
	switch exprType {
	case kindUpdate:
		return d.renderUpdate(tree, vals)
	case kindProjection:
		return "", errProjectionUnsupported
	default:
		return d.renderNode(tree, vals)
	}
}

// renderUpdate renders an update tree, which is an array of action nodes grouped
// by clause (SET / ADD / DELETE / REMOVE).
func (d *decodedExpression) renderUpdate(tree sexpr, vals []types.AttributeValue) (string, error) {
	if tree.children == nil {
		return "", errBadExpression
	}

	clauses := map[int][]string{}

	for _, action := range tree.children {
		code, body, err := d.renderAction(action, vals)
		if err != nil {
			return "", err
		}

		clauses[code] = append(clauses[code], body)
	}

	return joinUpdateClauses(clauses), nil
}

func (d *decodedExpression) renderAction(action sexpr, vals []types.AttributeValue) (int, string, error) {
	code, args, err := funcNode(action)
	if err != nil {
		return 0, "", err
	}

	switch code {
	case opSetAction:
		return renderBinaryAction(d, opSetAction, args, vals)
	case opAddAction:
		return renderBinaryAction(d, opAddAction, args, vals)
	case opDeleteAction:
		return renderBinaryAction(d, opDeleteAction, args, vals)
	case opRemoveAction:
		if len(args) != 1 {
			return 0, "", errBadExpression
		}

		p, e := d.renderNode(args[0], vals)

		return opRemoveAction, p, e
	default:
		return 0, "", errBadExpression
	}
}

func renderBinaryAction(
	d *decodedExpression,
	code int,
	args []sexpr,
	vals []types.AttributeValue,
) (int, string, error) {
	const binaryArity = 2
	if len(args) != binaryArity {
		return 0, "", errBadExpression
	}

	lhs, err := d.renderNode(args[0], vals)
	if err != nil {
		return 0, "", err
	}

	rhs, err := d.renderNode(args[1], vals)
	if err != nil {
		return 0, "", err
	}

	switch code {
	case opSetAction:
		return code, lhs + " = " + rhs, nil
	case opAddAction, opDeleteAction:
		return code, lhs + " " + rhs, nil
	default:
		return 0, "", errBadExpression
	}
}

func joinUpdateClauses(clauses map[int][]string) string {
	order := []struct {
		kw   string
		code int
	}{
		{"SET", opSetAction},
		{"ADD", opAddAction},
		{"DELETE", opDeleteAction},
		{"REMOVE", opRemoveAction},
	}

	var parts []string

	for _, c := range order {
		if items := clauses[c.code]; len(items) > 0 {
			parts = append(parts, c.kw+" "+strings.Join(items, ", "))
		}
	}

	return strings.Join(parts, " ")
}

// renderNode renders a single condition/value/path node to its DynamoDB string.
func (d *decodedExpression) renderNode(node sexpr, vals []types.AttributeValue) (string, error) {
	switch {
	case node.str != nil:
		return d.nameRef(*node.str), nil
	case node.children != nil:
		return d.renderCompound(node, vals)
	default:
		return "", errBadExpression
	}
}

func (d *decodedExpression) renderCompound(node sexpr, vals []types.AttributeValue) (string, error) {
	code, args, err := funcNode(node)
	if err != nil {
		return "", err
	}

	if r, ok, e := d.renderOperatorOrFunc(code, args, vals); ok || e != nil {
		return r, e
	}

	return "", errBadExpression
}

// renderOperatorOrFunc dispatches a compound node by its op code.
func (d *decodedExpression) renderOperatorOrFunc(
	code int, args []sexpr, vals []types.AttributeValue,
) (string, bool, error) {
	if sym, ok := comparatorSymbol(code); ok {
		s, e := d.renderBinaryOp(sym, args, vals)

		return s, true, e
	}

	switch code {
	case opVariable:
		s, e := d.renderVariable(args, vals)

		return s, true, e
	case opDocumentPath:
		s, e := d.renderDocumentPath(args, vals)

		return s, true, e
	case opAnd, opOr:
		s, e := d.renderLogical(code, args, vals)

		return s, true, e
	case opNot:
		s, e := d.renderNot(args, vals)

		return s, true, e
	case opBetween:
		s, e := d.renderBetween(args, vals)

		return s, true, e
	case opIn:
		s, e := d.renderIn(args, vals)

		return s, true, e
	case opPlus, opMinus:
		s, e := d.renderArithmetic(code, args, vals)

		return s, true, e
	default:
		return d.renderFunction(code, args, vals)
	}
}

func comparatorSymbol(code int) (string, bool) {
	switch code {
	case opEqual:
		return "=", true
	case opNotEqual:
		return "<>", true
	case opLessThan:
		return "<", true
	case opLessEqual:
		return "<=", true
	case opGreaterThan:
		return ">", true
	case opGreaterEqual:
		return ">=", true
	default:
		return "", false
	}
}

func (d *decodedExpression) renderBinaryOp(sym string, args []sexpr, vals []types.AttributeValue) (string, error) {
	const arity = 2
	if len(args) != arity {
		return "", errBadExpression
	}

	lhs, err := d.renderNode(args[0], vals)
	if err != nil {
		return "", err
	}

	rhs, err := d.renderNode(args[1], vals)
	if err != nil {
		return "", err
	}

	return lhs + " " + sym + " " + rhs, nil
}

func (d *decodedExpression) renderLogical(code int, args []sexpr, vals []types.AttributeValue) (string, error) {
	const arity = 2
	if len(args) != arity {
		return "", errBadExpression
	}

	lhs, err := d.renderNode(args[0], vals)
	if err != nil {
		return "", err
	}

	rhs, err := d.renderNode(args[1], vals)
	if err != nil {
		return "", err
	}

	kw := "AND"
	if code == opOr {
		kw = "OR"
	}

	return "(" + lhs + ") " + kw + " (" + rhs + ")", nil
}

func (d *decodedExpression) renderNot(args []sexpr, vals []types.AttributeValue) (string, error) {
	if len(args) != 1 {
		return "", errBadExpression
	}

	s, err := d.renderNode(args[0], vals)
	if err != nil {
		return "", err
	}

	return "NOT (" + s + ")", nil
}

func (d *decodedExpression) renderBetween(args []sexpr, vals []types.AttributeValue) (string, error) {
	const arity = 3
	if len(args) != arity {
		return "", errBadExpression
	}

	parts, err := d.renderAll(args, vals)
	if err != nil {
		return "", err
	}

	return parts[0] + " BETWEEN " + parts[1] + " AND " + parts[2], nil
}

func (d *decodedExpression) renderIn(args []sexpr, vals []types.AttributeValue) (string, error) {
	const minArity = 2
	if len(args) != minArity {
		return "", errBadExpression
	}

	lhs, err := d.renderNode(args[0], vals)
	if err != nil {
		return "", err
	}

	if args[1].children == nil {
		return "", errBadExpression
	}

	elems, err := d.renderAll(args[1].children, vals)
	if err != nil {
		return "", err
	}

	return lhs + " IN (" + strings.Join(elems, ", ") + ")", nil
}

func (d *decodedExpression) renderArithmetic(code int, args []sexpr, vals []types.AttributeValue) (string, error) {
	const arity = 2
	if len(args) != arity {
		return "", errBadExpression
	}

	lhs, err := d.renderNode(args[0], vals)
	if err != nil {
		return "", err
	}

	rhs, err := d.renderNode(args[1], vals)
	if err != nil {
		return "", err
	}

	op := "+"
	if code == opMinus {
		op = "-"
	}

	return lhs + " " + op + " " + rhs, nil
}

func (d *decodedExpression) renderVariable(args []sexpr, vals []types.AttributeValue) (string, error) {
	if len(args) != 1 || args[0].num == nil {
		return "", errBadExpression
	}

	id := int(*args[0].num)
	if id < 0 || id >= len(vals) {
		return "", errBadExpression
	}

	return d.valueRef(vals[id]), nil
}

// renderDocumentPath renders a nested attribute path: name, name.name, name[0],
// using #placeholders for each name element.
func (d *decodedExpression) renderDocumentPath(args []sexpr, _ []types.AttributeValue) (string, error) {
	if len(args) == 0 {
		return "", errBadExpression
	}

	var b strings.Builder

	for i, elem := range args {
		switch {
		case elem.str != nil:
			if i > 0 {
				b.WriteByte('.')
			}

			b.WriteString(d.nameRef(*elem.str))
		case elem.listIdx != nil:
			b.WriteByte('[')
			b.WriteString(strconv.FormatInt(*elem.listIdx, base10))
			b.WriteByte(']')
		default:
			return "", errBadExpression
		}
	}

	return b.String(), nil
}

func (d *decodedExpression) renderFunction(code int, args []sexpr, vals []types.AttributeValue) (string, bool, error) {
	name, ok := functionName(code)
	if !ok {
		return "", false, nil
	}

	parts, err := d.renderAll(args, vals)
	if err != nil {
		return "", true, err
	}

	return name + "(" + strings.Join(parts, ", ") + ")", true, nil
}

func functionName(code int) (string, bool) {
	switch code {
	case opAttributeExists:
		return "attribute_exists", true
	case opAttributeNotExists:
		return "attribute_not_exists", true
	case opAttributeType:
		return "attribute_type", true
	case opBeginsWith:
		return "begins_with", true
	case opContains:
		return "contains", true
	case opSize:
		return "size", true
	case opIfNotExists:
		return "if_not_exists", true
	case opListAppend:
		return "list_append", true
	default:
		return "", false
	}
}

func (d *decodedExpression) renderAll(nodes []sexpr, vals []types.AttributeValue) ([]string, error) {
	out := make([]string, len(nodes))

	for i, n := range nodes {
		s, err := d.renderNode(n, vals)
		if err != nil {
			return nil, err
		}

		out[i] = s
	}

	return out, nil
}

// funcNode extracts the leading function code and operands of a compound node.
func funcNode(node sexpr) (int, []sexpr, error) {
	if len(node.children) == 0 || node.children[0].num == nil {
		return 0, nil, errBadExpression
	}

	return int(*node.children[0].num), node.children[1:], nil
}
