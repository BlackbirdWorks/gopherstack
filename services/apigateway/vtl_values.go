package apigateway

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// vtlMap is an insertion-ordered map, matching the LinkedHashMap API Gateway
// builds from JSON so rendering and re-serialisation keep key order.
type vtlMap struct {
	items map[string]any
	keys  []string
	// emptyForMissing makes an absent key render as "" instead of its reference text.
	emptyForMissing bool
}

func newVTLMap() *vtlMap { return &vtlMap{items: map[string]any{}} }

func (m *vtlMap) put(k string, v any) {
	if _, ok := m.items[k]; !ok {
		m.keys = append(m.keys, k)
	}

	m.items[k] = v
}

func (m *vtlMap) get(k string) (any, bool) {
	v, ok := m.items[k]

	return v, ok
}

func (m *vtlMap) remove(k string) any {
	v, ok := m.items[k]
	if !ok {
		return nil
	}

	delete(m.items, k)

	for i, key := range m.keys {
		if key == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)

			break
		}
	}

	return v
}

// vtlList is a mutable list, so templates can build arrays with add().
type vtlList struct {
	items []any
}

const jsonNull = "null"

var errVTLJSON = errors.New("invalid JSON")

// parseOrderedJSON decodes JSON into vtl values (ordered maps, lists, int64/float64, string, bool, nil).
func parseOrderedJSON(s string) (any, error) {
	dec := jsontext.NewDecoder(strings.NewReader(s), jsonx.V1())

	v, err := decodeVTLValue(dec)
	if err != nil {
		return nil, err
	}

	if _, err = dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, errVTLJSON
	}

	return v, nil
}

func decodeVTLValue(dec *jsontext.Decoder) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}

	switch tok.Kind() {
	case '{':
		return decodeVTLObject(dec)
	case '[':
		return decodeVTLArray(dec)
	case '0':
		return numberFromJSON(jsonx.Number(tok.String())), nil
	case '"':
		return tok.String(), nil
	case 't', 'f':
		return tok.Bool(), nil
	default:
		var null any

		return null, nil
	}
}

func decodeVTLObject(dec *jsontext.Decoder) (any, error) {
	m := newVTLMap()

	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}

		if keyTok.Kind() != '"' {
			return nil, errVTLJSON
		}

		key := keyTok.String()

		v, err := decodeVTLValue(dec)
		if err != nil {
			return nil, err
		}

		m.put(key, v)
	}

	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}

	return m, nil
}

func decodeVTLArray(dec *jsontext.Decoder) (any, error) {
	l := &vtlList{items: []any{}}

	for dec.PeekKind() != ']' {
		v, err := decodeVTLValue(dec)
		if err != nil {
			return nil, err
		}

		l.items = append(l.items, v)
	}

	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}

	return l, nil
}

func numberFromJSON(n jsonx.Number) any {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return i
	}

	f, _ := n.Float64()

	return f
}

// encodeVTLJSON renders a vtl value as compact JSON, keeping key order.
func encodeVTLJSON(v any) string {
	var buf bytes.Buffer

	writeVTLJSON(&buf, v)

	return buf.String()
}

func writeVTLJSON(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil:
		buf.WriteString(jsonNull)
	case *vtlMap:
		buf.WriteByte('{')

		for i, k := range t.keys {
			if i > 0 {
				buf.WriteByte(',')
			}

			writeJSONString(buf, k)
			buf.WriteByte(':')
			writeVTLJSON(buf, t.items[k])
		}

		buf.WriteByte('}')
	case *vtlList:
		buf.WriteByte('[')

		for i, e := range t.items {
			if i > 0 {
				buf.WriteByte(',')
			}

			writeVTLJSON(buf, e)
		}

		buf.WriteByte(']')
	case string:
		writeJSONString(buf, t)
	case bool, int64, int:
		fmt.Fprint(buf, t)
	case float64:
		buf.WriteString(formatVTLFloat(t))
	default:
		writeJSONString(buf, javaString(t))
	}
}

func writeJSONString(buf *bytes.Buffer, s string) {
	var tmp bytes.Buffer

	enc := jsontext.NewEncoder(&tmp, jsonx.V1(), jsontext.EscapeForHTML(false))
	_ = json.MarshalEncode(enc, s)
	buf.Write(bytes.TrimRight(tmp.Bytes(), "\n"))
}

func formatVTLFloat(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatFloat(f, 'f', 1, 64)
	}

	return strconv.FormatFloat(f, 'f', -1, 64)
}

// javaString renders a value the way Java's toString() would, which is what
// Velocity prints for a reference.
func javaString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return formatVTLFloat(t)
	case *vtlMap:
		parts := make([]string, 0, len(t.keys))
		for _, k := range t.keys {
			parts = append(parts, k+"="+javaString(t.items[k]))
		}

		return "{" + strings.Join(parts, ", ") + "}"
	case *vtlList:
		parts := make([]string, len(t.items))
		for i, e := range t.items {
			parts[i] = javaString(e)
		}

		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprint(t)
	}
}

func stringMapToVTL(in map[string]string) *vtlMap {
	m := newVTLMap()

	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		m.put(k, in[k])
	}

	return m
}

// vtlToNumber converts a vtl value to int64/float64 when it is numeric.
func vtlToNumber(v any) (any, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return t, true
	}

	return nil, false
}

func vtlTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	}

	return true
}
