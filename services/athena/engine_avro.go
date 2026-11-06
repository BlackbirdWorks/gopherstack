package athena

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
)

const (
	avroSerdeLib       = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
	avroInputFmt       = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
	avroOutputFmt      = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
	fmtNameAvro        = "AVRO"
	avroRootName       = "hive_schema"
	defaultDecimalPrec = 10
	hiveArray          = "array"
	hiveMap            = "map"
	hiveStruct         = "struct"
	hiveDecimal        = "decimal"
	avroTimeLayout     = "2006-01-02 15:04:05.999999999"
)

var (
	errAvroType  = errors.New("unsupported Avro column type")
	errAvroValue = errors.New("cannot encode Avro value")
)

type hiveField struct {
	typ  *hiveType
	name string
}

type hiveType struct {
	elem   *hiveType
	base   string
	name   string
	fields []hiveField
	prec   int
	scale  int
}

func splitTop(s string, sep byte) []string {
	var (
		parts []string
		depth int
		start int
	)

	for i := range len(s) {
		switch c := s[i]; {
		case c == '<' || c == '(':
			depth++
		case c == '>' || c == ')':
			depth--
		case c == sep && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}

	return append(parts, s[start:])
}

func parseHiveType(raw string) (*hiveType, error) {
	t := strings.ToLower(strings.TrimSpace(raw))
	base, rest, hasArgs := strings.Cut(t, "<")

	if !hasArgs {
		base, rest, hasArgs = strings.Cut(t, "(")
		if hasArgs {
			rest = strings.TrimSuffix(rest, ")")
		}
	} else {
		rest = strings.TrimSuffix(rest, ">")
	}

	base = strings.TrimSpace(base)
	ht := &hiveType{base: base}

	switch base {
	case hiveArray:
		el, err := parseHiveType(rest)
		ht.elem = el

		return ht, err
	case hiveMap:
		kv := splitTop(rest, ',')
		if len(kv) != argsTwo {
			return nil, fmt.Errorf("%w: %s", errAvroType, raw)
		}

		el, err := parseHiveType(kv[1])
		ht.elem = el

		return ht, err
	case hiveStruct:
		for _, f := range splitTop(rest, ',') {
			name, typ, ok := strings.Cut(strings.TrimSpace(f), ":")
			if !ok {
				return nil, fmt.Errorf("%w: %s", errAvroType, raw)
			}

			ft, err := parseHiveType(typ)
			if err != nil {
				return nil, err
			}

			ht.fields = append(ht.fields, hiveField{name: strings.TrimSpace(name), typ: ft})
		}

		return ht, nil
	case hiveDecimal, typeNumeric:
		ht.base, ht.prec = hiveDecimal, defaultDecimalPrec

		if hasArgs {
			ps := strings.Split(rest, ",")
			ht.prec, _ = strconv.Atoi(strings.TrimSpace(ps[0]))

			if len(ps) > 1 {
				ht.scale, _ = strconv.Atoi(strings.TrimSpace(ps[1]))
			}
		}

		return ht, nil
	}

	return ht, nil
}

type avroNamer struct{ n int }

func (a *avroNamer) next(prefix string) string {
	a.n++

	return prefix + strconv.Itoa(a.n)
}

func nullable(v any) []any { return []any{"null", v} }

func (t *hiveType) avroSchema(nm *avroNamer) (any, error) {
	switch t.base {
	case typeBoolean:
		return "boolean", nil
	case typeTinyint, typeSmallint, typeInt, typeInteger:
		return "int", nil
	case typeBigint:
		return "long", nil
	case typeFloat, typeReal:
		return "float", nil
	case typeDouble:
		return "double", nil
	case columnTypeString, typeVarchar, "char":
		return "string", nil
	case "binary", "varbinary":
		return "bytes", nil
	case typeDate:
		return map[string]any{"type": "int", "logicalType": "date"}, nil
	case typeTimestamp:
		return map[string]any{"type": "long", "logicalType": "timestamp-millis"}, nil
	case hiveDecimal:
		return map[string]any{
			"type": "bytes", "logicalType": "decimal", "precision": t.prec, "scale": t.scale,
		}, nil
	case hiveArray:
		el, err := t.elem.avroSchema(nm)

		return map[string]any{"type": "array", "items": nullable(el)}, err
	case hiveMap:
		el, err := t.elem.avroSchema(nm)

		return map[string]any{"type": "map", "values": nullable(el)}, err
	case hiveStruct:
		fields := make([]any, 0, len(t.fields))

		for _, f := range t.fields {
			ft, err := f.typ.avroSchema(nm)
			if err != nil {
				return nil, err
			}

			fields = append(fields, map[string]any{"name": f.name, "type": nullable(ft), "default": nil})
		}

		t.name = nm.next("struct_")

		return map[string]any{"type": "record", "name": t.name, "fields": fields}, nil
	default:
		return nil, fmt.Errorf("%w: %s", errAvroType, t.base)
	}
}

func avroSchemaFor(cols []outCol) (string, []*hiveType, error) {
	nm := &avroNamer{}
	fields := make([]any, 0, len(cols))
	types := make([]*hiveType, len(cols))

	for i, c := range cols {
		ht, err := parseHiveType(c.typ)
		if err != nil {
			return "", nil, err
		}

		sch, err := ht.avroSchema(nm)
		if err != nil {
			return "", nil, fmt.Errorf("column %s: %w", c.name, err)
		}

		types[i] = ht
		fields = append(fields, map[string]any{"name": c.name, "type": nullable(sch), "default": nil})
	}

	b, err := json.Marshal(map[string]any{"type": "record", "name": avroRootName, "fields": fields})

	return string(b), types, err
}

func avroTime(s string) (time.Time, error) {
	if t, err := time.Parse(dateLayout, s); err == nil {
		return t, nil
	}

	return time.Parse(avroTimeLayout, s)
}

func (t *hiveType) fromText(s string) (any, error) {
	switch t.base {
	case typeBoolean:
		return s == strTrue, nil
	case typeTinyint, typeSmallint, typeInt, typeInteger:
		n, err := strconv.ParseInt(s, 10, 32)

		return int32(n), err
	case typeBigint:
		return strconv.ParseInt(s, 10, 64)
	case typeFloat, typeReal:
		f, err := strconv.ParseFloat(s, 32)

		return float32(f), err
	case typeDouble:
		return strconv.ParseFloat(s, 64)
	case "binary", "varbinary":
		return []byte(s), nil
	case typeDate, typeTimestamp:
		return avroTime(s)
	case hiveDecimal:
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			return nil, fmt.Errorf("%w: decimal %q", errAvroValue, s)
		}

		return r, nil
	case hiveArray, hiveMap, hiveStruct:
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()

		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("%w: %w", errAvroValue, err)
		}

		return t.fromJSON(v)
	default:
		return s, nil
	}
}

func jsonScalarText(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case json.Number:
		return n.String()
	case bool:
		return strconv.FormatBool(n)
	default:
		return fmt.Sprint(n)
	}
}

func (t *hiveType) fromJSON(v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // JSON null is an Avro null
	}

	switch t.base {
	case hiveArray:
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: expected array", errAvroValue)
		}

		out := make([]any, len(arr))

		for i, e := range arr {
			ev, err := t.elem.fromJSON(e)
			if err != nil {
				return nil, err
			}

			out[i] = t.elem.union(ev)
		}

		return out, nil
	case hiveMap:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: expected map", errAvroValue)
		}

		out := make(map[string]any, len(m))

		for k, e := range m {
			ev, err := t.elem.fromJSON(e)
			if err != nil {
				return nil, err
			}

			out[k] = t.elem.union(ev)
		}

		return out, nil
	case hiveStruct:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: expected struct", errAvroValue)
		}

		lower := make(map[string]any, len(m))
		for k, e := range m {
			lower[strings.ToLower(k)] = e
		}

		out := make(map[string]any, len(t.fields))

		for _, f := range t.fields {
			ev, err := f.typ.fromJSON(lower[strings.ToLower(f.name)])
			if err != nil {
				return nil, err
			}

			out[f.name] = f.typ.union(ev)
		}

		return out, nil
	default:
		return t.fromText(jsonScalarText(v))
	}
}

// union wraps a map or record value for its ["null", T] slot: the encoder
// reads a bare map[string]any there as a union branch selector.
func (t *hiveType) union(v any) any {
	switch {
	case v == nil:
		return nil
	case t.base == hiveMap:
		return map[string]any{hiveMap: v}
	case t.base == hiveStruct:
		return map[string]any{t.name: v}
	default:
		return v
	}
}

func avroCodec(name string) (ocf.CodecName, error) {
	switch strings.ToUpper(name) {
	case "", compressionNone:
		return ocf.Null, nil
	case "DEFLATE":
		return ocf.Deflate, nil
	case "SNAPPY":
		return ocf.Snappy, nil
	default:
		return "", fmt.Errorf("%w: compression %s", errAvroType, name)
	}
}

// encodeAvroRows writes one Avro object container file holding the given rows.
func encodeAvroRows(res *sqlResult, rows []int, cols []outCol, codec string) ([]byte, error) {
	schema, types, err := avroSchemaFor(cols)
	if err != nil {
		return nil, err
	}

	cn, err := avroCodec(codec)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(schema, &buf, ocf.WithCodec(cn))
	if err != nil {
		return nil, err
	}

	for _, r := range rows {
		rec := make(map[string]any, len(cols))

		for i, c := range cols {
			v, null := res.cell(r, c.src)
			if null {
				rec[c.name] = nil

				continue
			}

			av, perr := types[i].fromText(v)
			if perr != nil {
				return nil, fmt.Errorf("column %s: %w", c.name, perr)
			}

			rec[c.name] = types[i].union(av)
		}

		if err = enc.Encode(rec); err != nil {
			return nil, err
		}
	}

	if err = enc.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// avroNorm turns a decoded Avro value into JSON-friendly Go values typed by t (nil: untyped).
func avroNorm(v any, t *hiveType) any {
	switch n := v.(type) {
	case time.Time:
		if t != nil && t.base == typeDate {
			return n.UTC().Format(dateLayout)
		}

		return n.UTC().Format(tsMillisLayout)
	case *big.Rat:
		scale := 0
		if t != nil {
			scale = t.scale
		}

		return n.FloatString(scale)
	case []byte:
		return string(n)
	case [16]byte:
		return fmt.Sprintf("%x", n[:])
	case float32:
		return float64(n)
	case []any:
		out := make([]any, len(n))

		var el *hiveType
		if t != nil {
			el = t.elem
		}

		for i, e := range n {
			out[i] = avroNorm(e, el)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(n))

		for k, e := range n {
			out[k] = avroNorm(e, subType(t, k))
		}

		return out
	default:
		return v
	}
}

func subType(t *hiveType, field string) *hiveType {
	if t == nil {
		return nil
	}

	if t.base == hiveStruct {
		for _, f := range t.fields {
			if strings.EqualFold(f.name, field) {
				return f.typ
			}
		}

		return nil
	}

	return t.elem
}

func derefSchema(s avro.Schema) avro.Schema {
	if ref, ok := s.(*avro.RefSchema); ok {
		return ref.Schema()
	}

	return s
}

func unionKey(s avro.Schema) string {
	switch sc := derefSchema(s).(type) {
	case *avro.ArraySchema:
		return string(avro.Array)
	case *avro.MapSchema:
		return string(avro.Map)
	case *avro.RecordSchema:
		return sc.FullName()
	default:
		return ""
	}
}

// avroUnwrap strips the {branch: value} wrapper the decoder puts around
// array, map and record members of a union, guided by the file's schema.
func avroUnwrap(v any, s avro.Schema) any {
	switch sc := derefSchema(s).(type) {
	case *avro.UnionSchema:
		return unwrapUnion(v, sc)
	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}

		for _, f := range sc.Fields() {
			if fv, has := m[f.Name()]; has {
				m[f.Name()] = avroUnwrap(fv, f.Type())
			}
		}

		return m
	case *avro.ArraySchema:
		if arr, ok := v.([]any); ok {
			for i, e := range arr {
				arr[i] = avroUnwrap(e, sc.Items())
			}
		}

		return v
	case *avro.MapSchema:
		if m, ok := v.(map[string]any); ok {
			for k, e := range m {
				m[k] = avroUnwrap(e, sc.Values())
			}
		}

		return v
	default:
		return v
	}
}

func unwrapUnion(v any, sc *avro.UnionSchema) any {
	if v == nil {
		return nil
	}

	var only avro.Schema

	nonNull := 0

	for _, br := range sc.Types() {
		if br.Type() == avro.Null {
			continue
		}

		nonNull++
		only = br

		if m, ok := v.(map[string]any); ok && len(m) == 1 {
			if inner, has := m[unionKey(br)]; has && unionKey(br) != "" {
				return avroUnwrap(inner, br)
			}
		}
	}

	if nonNull == 1 {
		return avroUnwrap(v, only)
	}

	return v
}

func avroCell(v any, t *hiveType) string {
	nv := avroNorm(v, t)

	switch n := nv.(type) {
	case nil:
		return ""
	case string:
		return n
	case bool:
		return strconv.FormatBool(n)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int64:
		return avroIntCell(n, t)
	case int:
		return strconv.Itoa(n)
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return formatFloat(n)
		}

		return strconv.FormatFloat(n, 'g', -1, 64)
	default:
		b, err := json.Marshal(n)
		if err != nil {
			return fmt.Sprint(n)
		}

		return string(b)
	}
}

func avroIntCell(n int64, t *hiveType) string {
	if t != nil && t.base == typeTimestamp {
		return time.UnixMilli(n).UTC().Format(tsMillisLayout)
	}

	return strconv.FormatInt(n, 10)
}

func (l *tableLoader) decodeAvro(ctx context.Context, rd io.Reader, cols []Column, sink *rowSink) error {
	dec, err := ocf.NewDecoder(rd)
	if err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: invalid Avro file: %w", err)
	}

	types := make([]*hiveType, len(cols))

	for i, c := range cols {
		if ht, perr := parseHiveType(c.Type); perr == nil {
			types[i] = ht
		}
	}

	for dec.HasNext() {
		var obj map[string]any
		if err = dec.Decode(&obj); err != nil {
			return fmt.Errorf("HIVE_CURSOR_ERROR: Failed to read Avro record: %w", err)
		}

		obj, _ = avroUnwrap(obj, dec.Schema()).(map[string]any)

		lower := make(map[string]any, len(obj))
		for k, v := range obj {
			lower[strings.ToLower(k)] = v
		}

		rec := make([]string, len(cols))
		nulls := make([]bool, len(cols))

		for i, c := range cols {
			v, ok := lower[strings.ToLower(c.Name)]
			nulls[i] = !ok || v == nil

			if !nulls[i] {
				rec[i] = avroCell(v, types[i])
			}
		}

		if err = l.insertJSON(ctx, sink, rec, nulls); err != nil {
			return err
		}
	}

	if err = dec.Error(); err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: %w", err)
	}

	return nil
}

