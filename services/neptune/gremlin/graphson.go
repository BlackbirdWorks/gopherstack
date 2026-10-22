package gremlin

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

const (
	mimeGraphSONv3 = "application/vnd.gremlin-v3.0+json"
	mimeGraphSONv2 = "application/vnd.gremlin-v2.0+json"
	mimeJSON       = "application/json"
)

type graphsonWriter struct {
	v2 bool
}

func typed(t string, v any) map[string]any {
	return map[string]any{"@type": t, "@value": v}
}

func (w graphsonWriter) value(v any) any {
	switch t := v.(type) {
	case nil, bool, string:
		return t
	case int64:
		return typed("g:Int64", t)
	case int32:
		return typed("g:Int32", t)
	case int:
		return typed("g:Int32", t)
	case float64:
		return typed("g:Double", floatJSON(t))
	case float32:
		return typed("g:Float", floatJSON(float64(t)))
	case []any:
		return w.list("g:List", t)
	case Set:
		return w.list("g:Set", t)
	case *Map:
		return w.mapValue(t)
	case Token:
		return typed("g:"+t.Kind, t.Name)
	}

	return w.element(v)
}

func floatJSON(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	return f
}

func (w graphsonWriter) list(typ string, items []any) any {
	out := make([]any, len(items))
	for i, e := range items {
		out[i] = w.value(e)
	}

	if w.v2 {
		return out
	}

	return typed(typ, out)
}

func (w graphsonWriter) mapValue(m *Map) any {
	if w.v2 {
		return w.mapV2(m)
	}

	flat := make([]any, 0, m.Len()+m.Len())

	for i := range m.Len() {
		k, v := m.Entry(i)
		flat = append(flat, w.value(k), w.value(v))
	}

	return typed("g:Map", flat)
}

func (w graphsonWriter) mapV2(m *Map) any {
	out := make(map[string]any, m.Len())

	for i := range m.Len() {
		k, v := m.Entry(i)
		out[mapKeyString(k)] = w.value(v)
	}

	return out
}

func mapKeyString(k any) string {
	switch t := k.(type) {
	case string:
		return t
	case Token:
		return t.Name
	case *Vertex:
		return t.ID
	case int64:
		return strconv.FormatInt(t, 10)
	}

	return keyOf(k)
}

func (w graphsonWriter) element(v any) any {
	switch t := v.(type) {
	case *Vertex:
		return typed("g:Vertex", map[string]any{tokID: t.ID, tokLabel: t.Label})
	case *Edge:
		return typed("g:Edge", map[string]any{
			tokID: t.ID, tokLabel: t.Label,
			"inVLabel": t.InLabel, "outVLabel": t.OutLabel, "inV": t.InID, "outV": t.OutID,
		})
	case *Property:
		return w.property(t)
	case *Path:
		return w.path(t)
	}

	return typed("g:String", keyOf(v))
}

func (w graphsonWriter) property(p *Property) any {
	if p.VP {
		return typed("g:VertexProperty", map[string]any{
			tokID: w.value(p.ID), tokValue: w.value(p.Val), tokLabel: p.Key,
		})
	}

	return typed("g:Property", map[string]any{tokKey: p.Key, tokValue: w.value(p.Val)})
}

func (w graphsonWriter) path(p *Path) any {
	labels := make([]any, len(p.Labels))

	for i, ls := range p.Labels {
		set := make([]any, len(ls))
		for j, l := range ls {
			set[j] = l
		}

		labels[i] = w.list("g:Set", set)
	}

	return typed("g:Path", map[string]any{
		"labels": w.list("g:List", labels), "objects": w.list("g:List", p.Objects),
	})
}

// graphsonResponse renders one response message.
func (w graphsonWriter) response(id string, code int, message string, data []any, hasData bool) ([]byte, error) {
	result := map[string]any{"meta": w.emptyMap()}

	if hasData {
		result["data"] = w.list("g:List", data)
	} else {
		result["data"] = nil
	}

	msg := map[string]any{
		jsonReqID: id,
		"status":  map[string]any{"message": message, jsonCode: code, "attributes": w.emptyMap()},
		"result":  result,
	}

	return marshalNoEscape(msg)
}

func (w graphsonWriter) emptyMap() any {
	if w.v2 {
		return map[string]any{}
	}

	return typed("g:Map", []any{})
}

func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(v); err != nil {
		return nil, err //nolint:wrapcheck // plain encode error
	}

	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// decodeGraphSON turns a decoded JSON request value (numbers as json.Number) into engine values.
func decodeGraphSON(v any) (any, error) {
	switch t := v.(type) {
	case json.Number:
		return numberFromJSON(t), nil
	case []any:
		return decodeGraphSONList(t)
	case map[string]any:
		typ, ok := t["@type"].(string)
		if !ok {
			return decodeGraphSONPlainMap(t)
		}

		return decodeTyped(typ, t["@value"])
	}

	return v, nil
}

func numberFromJSON(n json.Number) any {
	if i, err := n.Int64(); err == nil {
		return i
	}

	f, _ := n.Float64()

	return f
}

func decodeGraphSONList(l []any) ([]any, error) {
	out := make([]any, len(l))

	for i, e := range l {
		d, err := decodeGraphSON(e)
		if err != nil {
			return nil, err
		}

		out[i] = d
	}

	return out, nil
}

func decodeGraphSONPlainMap(m map[string]any) (any, error) {
	out := NewMap()

	for k, e := range m {
		d, err := decodeGraphSON(e)
		if err != nil {
			return nil, err
		}

		out.Put(k, d)
	}

	return out, nil
}

func decodeTyped(typ string, val any) (any, error) {
	switch typ {
	case "g:Int32", "g:Int64", "g:Float", "g:Double", "g:Short", "g:Byte", "g:Date", "g:Timestamp", "g:BigInteger":
		return decodeNumber(val)
	case "g:List":
		l, _ := val.([]any)

		return decodeGraphSONList(l)
	case "g:Set":
		l, _ := val.([]any)
		out, err := decodeGraphSONList(l)

		return Set(out), err
	case "g:Map":
		return decodeGraphSONPairs(val)
	case "g:UUID", "g:String":
		return val, nil
	case "g:Bytecode":
		return decodeBytecodeJSON(val)
	case "g:P", "g:TextP":
		return decodePredJSON(val)
	case "g:Vertex", "g:Edge":
		return decodeElementRef(val)
	case "g:Lambda":
		return nil, unsupported("Lambdas are not supported")
	}

	if name, ok := val.(string); ok {
		return Token{Kind: typ[len("g:"):], Name: name}, nil
	}

	return nil, unsupported("Unsupported GraphSON type %q", typ)
}

func decodeNumber(val any) (any, error) {
	switch t := val.(type) {
	case json.Number:
		return numberFromJSON(t), nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return nil, invalidArg("Invalid number %q", t)
		}

		return f, nil
	}

	return nil, invalidArg("Invalid numeric value")
}

func decodeGraphSONPairs(val any) (any, error) {
	l, _ := val.([]any)
	out := NewMap()

	for i := 0; i+1 < len(l); i += 2 {
		k, err := decodeGraphSON(l[i])
		if err != nil {
			return nil, err
		}

		v, err := decodeGraphSON(l[i+1])
		if err != nil {
			return nil, err
		}

		out.Put(k, v)
	}

	return out, nil
}

func decodeElementRef(val any) (any, error) {
	m, _ := val.(map[string]any)

	id, err := decodeGraphSON(m[tokID])
	if err != nil {
		return nil, err
	}

	return VertexRef{ID: idString(id)}, nil
}

func decodeBytecodeJSON(val any) (any, error) {
	m, _ := val.(map[string]any)
	steps, _ := m["step"].([]any)
	tr := &Traversal{}

	for _, raw := range steps {
		parts, _ := raw.([]any)
		if len(parts) == 0 {
			continue
		}

		name, _ := parts[0].(string)

		args, err := decodeGraphSONList(parts[1:])
		if err != nil {
			return nil, err
		}

		tr.add(Step{Name: name, Args: args})
	}

	return tr, nil
}

func decodePredJSON(val any) (any, error) {
	m, _ := val.(map[string]any)
	op, _ := m["predicate"].(string)

	v, err := decodeGraphSON(m[tokValue])
	if err != nil {
		return nil, err
	}

	return buildPred(op, v, m["other"])
}

func buildPred(op string, value any, other any) (Pred, error) {
	if op == opAnd || op == opOr {
		return connective(op, value)
	}

	p := Pred{Op: op}

	switch op {
	case opWithin, opWithout:
		p.Args = []any{value}
	case opBetween, opInside, opOutside:
		l, ok := value.([]any)
		if !ok {
			second, err := decodeGraphSON(other)
			if err != nil {
				return p, err
			}

			l = []any{value, second}
		}

		p.Args = l
	default:
		p.Args = []any{value}
	}

	return p, nil
}

func connective(op string, value any) (Pred, error) {
	p := Pred{Op: op}

	l, _ := value.([]any)
	for _, e := range l {
		c, ok := e.(Pred)
		if !ok {
			return p, invalidArg("%s() requires predicates", op)
		}

		p.Children = append(p.Children, c)
	}

	return p, nil
}
