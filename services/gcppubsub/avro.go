package gcppubsub

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
)

var errAvro = errors.New("avro")

const (
	maxAvroBlock = 1 << 20
	float32Bytes = 4
	float64Bytes = 8
)

type avroKind string

const (
	avroNull    avroKind = "null"
	avroBoolean avroKind = "boolean"
	avroInt     avroKind = "int"
	avroLong    avroKind = "long"
	avroFloat   avroKind = "float"
	avroDouble  avroKind = "double"
	avroBytes   avroKind = "bytes"
	avroString  avroKind = "string"
	avroRecord  avroKind = "record"
	avroEnum    avroKind = "enum"
	avroArray   avroKind = "array"
	avroMap     avroKind = "map"
	avroFixed   avroKind = "fixed"
	avroUnion   avroKind = "union"
)

type avroField struct {
	typ        *avroType
	name       string
	hasDefault bool
}

type avroType struct {
	elem     *avroType
	kind     avroKind
	name     string
	fields   []avroField
	symbols  []string
	branches []*avroType
	size     int
}

func avroErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errAvro, fmt.Sprintf(format, a...))
}

func primitiveKind(s string) (avroKind, bool) {
	switch k := avroKind(s); k {
	case avroNull, avroBoolean, avroInt, avroLong, avroFloat, avroDouble, avroBytes, avroString:
		return k, true
	default:
		return "", false
	}
}

// parseAvroSchema parses an Avro schema JSON document.
func parseAvroSchema(def string) (*avroType, error) {
	var raw any

	if err := json.Unmarshal([]byte(def), &raw); err != nil {
		return nil, avroErr("schema is not valid JSON: %v", err)
	}

	return parseAvro(raw, make(map[string]*avroType), "")
}

func parseAvro(v any, names map[string]*avroType, ns string) (*avroType, error) {
	switch x := v.(type) {
	case string:
		if k, ok := primitiveKind(x); ok {
			return &avroType{kind: k}, nil
		}

		if t, ok := names[x]; ok {
			return t, nil
		}

		if t, ok := names[ns+"."+x]; ok {
			return t, nil
		}

		return nil, avroErr("unknown type %q", x)
	case []any:
		return parseAvroUnion(x, names, ns)
	case map[string]any:
		return parseAvroObject(x, names, ns)
	default:
		return nil, avroErr("invalid schema node")
	}
}

func parseAvroUnion(branches []any, names map[string]*avroType, ns string) (*avroType, error) {
	if len(branches) == 0 {
		return nil, avroErr("union must have at least one branch")
	}

	u := &avroType{kind: avroUnion}

	for _, br := range branches {
		bt, err := parseAvro(br, names, ns)
		if err != nil {
			return nil, err
		}

		if bt.kind == avroUnion {
			return nil, avroErr("unions may not directly contain unions")
		}

		u.branches = append(u.branches, bt)
	}

	return u, nil
}

func parseAvroObject(o map[string]any, names map[string]*avroType, ns string) (*avroType, error) {
	typ, ok := o["type"]
	if !ok {
		return nil, avroErr("schema object is missing \"type\"")
	}

	ts, isStr := typ.(string)
	if !isStr {
		return parseAvro(typ, names, ns)
	}

	switch avroKind(ts) {
	case avroRecord:
		return parseAvroRecord(o, names, ns)
	case avroEnum:
		return parseAvroEnum(o, names, ns)
	case avroFixed:
		return parseAvroFixed(o, names, ns)
	case avroArray:
		items, err := parseAvro(o["items"], names, ns)
		if err != nil {
			return nil, err
		}

		return &avroType{kind: avroArray, elem: items}, nil
	case avroMap:
		values, err := parseAvro(o["values"], names, ns)
		if err != nil {
			return nil, err
		}

		return &avroType{kind: avroMap, elem: values}, nil
	default:
		return parseAvro(ts, names, ns)
	}
}

func registerAvroName(o map[string]any, names map[string]*avroType, ns string, t *avroType) (string, error) {
	name, _ := o["name"].(string)
	if name == "" {
		return "", avroErr("%s is missing \"name\"", t.kind)
	}

	if n, ok := o["namespace"].(string); ok && n != "" {
		ns = n
	}

	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		ns, name = name[:i], name[i+1:]
	}

	t.name = name
	if ns != "" {
		t.name = ns + "." + name
	}

	if _, dup := names[t.name]; dup {
		return "", avroErr("duplicate definition of %q", t.name)
	}

	names[t.name] = t
	names[name] = t

	return ns, nil
}

func parseAvroRecord(o map[string]any, names map[string]*avroType, ns string) (*avroType, error) {
	t := &avroType{kind: avroRecord}

	childNS, err := registerAvroName(o, names, ns, t)
	if err != nil {
		return nil, err
	}

	fields, _ := o["fields"].([]any)
	for _, f := range fields {
		fo, isMap := f.(map[string]any)
		if !isMap {
			return nil, avroErr("record %q has an invalid field", t.name)
		}

		fname, _ := fo["name"].(string)
		if fname == "" {
			return nil, avroErr("record %q has a field without a name", t.name)
		}

		ft, ferr := parseAvro(fo["type"], names, childNS)
		if ferr != nil {
			return nil, ferr
		}

		_, hasDefault := fo["default"]
		t.fields = append(t.fields, avroField{name: fname, typ: ft, hasDefault: hasDefault})
	}

	return t, nil
}

func parseAvroEnum(o map[string]any, names map[string]*avroType, ns string) (*avroType, error) {
	t := &avroType{kind: avroEnum}

	if _, err := registerAvroName(o, names, ns, t); err != nil {
		return nil, err
	}

	syms, _ := o["symbols"].([]any)
	if len(syms) == 0 {
		return nil, avroErr("enum %q must have symbols", t.name)
	}

	for _, s := range syms {
		str, ok := s.(string)
		if !ok {
			return nil, avroErr("enum %q has a non-string symbol", t.name)
		}

		t.symbols = append(t.symbols, str)
	}

	return t, nil
}

func parseAvroFixed(o map[string]any, names map[string]*avroType, ns string) (*avroType, error) {
	t := &avroType{kind: avroFixed}

	if _, err := registerAvroName(o, names, ns, t); err != nil {
		return nil, err
	}

	size, ok := o["size"].(float64)
	if !ok || size < 0 || size != math.Trunc(size) {
		return nil, avroErr("fixed %q needs a non-negative integer size", t.name)
	}

	t.size = int(size)

	return t, nil
}

func (t *avroType) branchName() string {
	if t.name != "" {
		return t.name
	}

	return string(t.kind)
}

// validJSON checks a value decoded from the Avro JSON encoding against the schema.
func (t *avroType) validJSON(v any) bool {
	switch t.kind {
	case avroArray:
		arr, ok := v.([]any)

		return ok && allValid(arr, t.elem)
	case avroMap:
		m, ok := v.(map[string]any)
		if !ok {
			return false
		}

		for _, e := range m {
			if !t.elem.validJSON(e) {
				return false
			}
		}

		return true
	case avroRecord:
		return t.validJSONRecord(v)
	case avroUnion:
		return t.validJSONUnion(v)
	default:
		return t.validJSONScalar(v)
	}
}

func allValid(vals []any, elem *avroType) bool {
	for _, e := range vals {
		if !elem.validJSON(e) {
			return false
		}
	}

	return true
}

func (t *avroType) validJSONScalar(v any) bool {
	switch t.kind {
	case avroNull:
		return v == nil
	case avroBoolean:
		_, ok := v.(bool)

		return ok
	case avroInt, avroLong, avroFloat, avroDouble:
		return t.validJSONNumber(v)
	case avroString, avroBytes:
		_, ok := v.(string)

		return ok
	case avroFixed:
		s, ok := v.(string)

		return ok && len([]rune(s)) == t.size
	case avroEnum:
		s, ok := v.(string)

		return ok && slices.Contains(t.symbols, s)
	default:
		return false
	}
}

func (t *avroType) validJSONNumber(v any) bool {
	f, ok := v.(float64)
	if !ok {
		return false
	}

	switch t.kind {
	case avroInt:
		return f == math.Trunc(f) && f >= math.MinInt32 && f <= math.MaxInt32
	case avroLong:
		return f == math.Trunc(f)
	default:
		return true
	}
}

func (t *avroType) validJSONRecord(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}

	for _, f := range t.fields {
		fv, present := m[f.name]
		if !present {
			if f.hasDefault || f.typ.acceptsNull() {
				continue
			}

			return false
		}

		if !f.typ.validJSON(fv) {
			return false
		}
	}

	return true
}

func (t *avroType) acceptsNull() bool {
	if t.kind == avroNull {
		return true
	}

	if t.kind != avroUnion {
		return false
	}

	for _, b := range t.branches {
		if b.kind == avroNull {
			return true
		}
	}

	return false
}

func (t *avroType) validJSONUnion(v any) bool {
	if v == nil {
		return t.acceptsNull()
	}

	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}

	for key, val := range m {
		for _, b := range t.branches {
			if b.branchName() == key || (b.name != "" && strings.HasSuffix(b.name, "."+key)) {
				return b.validJSON(val)
			}
		}
	}

	return false
}

type avroReader struct {
	buf []byte
}

func (r *avroReader) take(n int) ([]byte, bool) {
	if n < 0 || n > len(r.buf) {
		return nil, false
	}

	out := r.buf[:n]
	r.buf = r.buf[n:]

	return out, true
}

func (r *avroReader) long() (int64, bool) {
	u, n := binary.Uvarint(r.buf)
	if n <= 0 {
		return 0, false
	}

	r.buf = r.buf[n:]

	return int64(u>>1) ^ -int64(u&1), true
}

// consumeBinary reads one value of type t in the Avro binary encoding.
func (t *avroType) consumeBinary(r *avroReader) bool {
	switch t.kind {
	case avroUnion:
		i, ok := r.long()

		return ok && i >= 0 && i < int64(len(t.branches)) && t.branches[i].consumeBinary(r)
	case avroRecord:
		for _, f := range t.fields {
			if !f.typ.consumeBinary(r) {
				return false
			}
		}

		return true
	case avroArray, avroMap:
		return t.consumeBlocks(r)
	default:
		return t.consumeScalar(r)
	}
}

func (t *avroType) consumeScalar(r *avroReader) bool {
	switch t.kind {
	case avroNull:
		return true
	case avroBoolean:
		b, ok := r.take(1)

		return ok && b[0] <= 1
	case avroInt:
		v, ok := r.long()

		return ok && v >= math.MinInt32 && v <= math.MaxInt32
	case avroLong:
		_, ok := r.long()

		return ok
	case avroFloat, avroDouble:
		_, ok := r.take(t.floatBytes())

		return ok
	case avroBytes, avroString:
		return r.consumeBytes(t.kind == avroString)
	case avroFixed:
		_, ok := r.take(t.size)

		return ok
	case avroEnum:
		i, ok := r.long()

		return ok && i >= 0 && i < int64(len(t.symbols))
	default:
		return false
	}
}

func (r *avroReader) consumeBytes(utf8Only bool) bool {
	n, ok := r.long()
	if !ok || n < 0 || n > int64(len(r.buf)) {
		return false
	}

	b, _ := r.take(int(n))

	return !utf8Only || utf8.Valid(b)
}

func (t *avroType) consumeBlocks(r *avroReader) bool {
	for {
		n, ok := r.blockCount()
		if !ok {
			return false
		}

		if n == 0 {
			return true
		}

		for range n {
			if t.kind == avroMap && !r.consumeBytes(true) {
				return false
			}

			if !t.elem.consumeBinary(r) {
				return false
			}
		}
	}
}

// blockCount reads an array/map block header and returns the item count.
func (r *avroReader) blockCount() (int64, bool) {
	n, ok := r.long()
	if !ok {
		return 0, false
	}

	if n < 0 {
		n = -n

		if _, ok = r.long(); !ok {
			return 0, false
		}
	}

	return n, n <= maxAvroBlock
}

func (t *avroType) floatBytes() int {
	if t.kind == avroFloat {
		return float32Bytes
	}

	return float64Bytes
}
