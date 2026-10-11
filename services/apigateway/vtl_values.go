package apigateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
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
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	v, err := decodeVTLValue(dec)
	if err != nil {
		return nil, err
	}

	if _, err = dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errVTLJSON
	}

	return v, nil
}

func decodeVTLValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}

	switch t := tok.(type) {
	case json.Delim:
		if t == '{' {
			return decodeVTLObject(dec)
		}

		return decodeVTLArray(dec)
	case json.Number:
		return numberFromJSON(t), nil
	default:
		return t, nil
	}
}

func decodeVTLObject(dec *json.Decoder) (any, error) {
	m := newVTLMap()

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}

		key, ok := keyTok.(string)
		if !ok {
			return nil, errVTLJSON
		}

		v, err := decodeVTLValue(dec)
		if err != nil {
			return nil, err
		}

		m.put(key, v)
	}

	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	return m, nil
}

func decodeVTLArray(dec *json.Decoder) (any, error) {
	l := &vtlList{items: []any{}}

	for dec.More() {
		v, err := decodeVTLValue(dec)
		if err != nil {
			return nil, err
		}

		l.items = append(l.items, v)
	}

	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	return l, nil
}

func numberFromJSON(n json.Number) any {
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

	enc := json.NewEncoder(&tmp)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
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
