package athena

import (
	"bytes"
	"context"
	"encoding/hex"
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
	avroKeyType        = "type"
	avroKeyName        = "name"
	avroKeyDefault     = "default"
	avroKeyLogical     = "logicalType"
	codecDeflate       = "DEFLATE"
	codecSnappy        = "SNAPPY"
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

func splitTypeArgs(t string) (string, string, bool) {
	if base, rest, ok := strings.Cut(t, "<"); ok {
		return strings.TrimSpace(base), strings.TrimSuffix(rest, ">"), true
	}

	if base, rest, ok := strings.Cut(t, "("); ok {
		return strings.TrimSpace(base), strings.TrimSuffix(rest, ")"), true
	}

	return strings.TrimSpace(t), "", false
}

func parseHiveType(raw string) (*hiveType, error) {
	base, args, hasArgs := splitTypeArgs(strings.ToLower(strings.TrimSpace(raw)))
	ht := &hiveType{base: base}

	var err error

	switch base {
	case hiveArray:
		ht.elem, err = parseHiveType(args)
	case hiveMap:
		kv := splitTop(args, ',')
		if len(kv) != argsTwo {
			return nil, fmt.Errorf("%w: %s", errAvroType, raw)
		}

		ht.elem, err = parseHiveType(kv[1])
	case hiveStruct:
		ht.fields, err = parseStructFields(args, raw)
	case hiveDecimal, typeNumeric:
		ht.base, ht.prec = hiveDecimal, defaultDecimalPrec

		if hasArgs {
			ht.prec, ht.scale = parseDecimalArgs(args)
		}
	}

	return ht, err
}

func parseStructFields(args, raw string) ([]hiveField, error) {
	var out []hiveField

	for _, f := range splitTop(args, ',') {
		name, typ, ok := strings.Cut(strings.TrimSpace(f), ":")
		if !ok {
			return nil, fmt.Errorf("%w: %s", errAvroType, raw)
		}

		ft, err := parseHiveType(typ)
		if err != nil {
			return nil, err
		}

		out = append(out, hiveField{name: strings.TrimSpace(name), typ: ft})
	}

	return out, nil
}

func parseDecimalArgs(args string) (int, int) {
	ps := strings.Split(args, ",")
	prec, _ := strconv.Atoi(strings.TrimSpace(ps[0]))
	scale := 0

	if len(ps) > 1 {
		scale, _ = strconv.Atoi(strings.TrimSpace(ps[1]))
	}

	return prec, scale
}

type avroNamer struct{ n int }

func (a *avroNamer) next(prefix string) string {
	a.n++

	return prefix + strconv.Itoa(a.n)
}

func nullable(v any) []any { return []any{kwNull, v} }

func avroPrimitiveSchema(base string) (any, bool) {
	switch base {
	case typeBoolean:
		return "boolean", true
	case typeTinyint, typeSmallint, typeInt, typeInteger:
		return "int", true
	case typeBigint:
		return "long", true
	case typeFloat, typeReal:
		return "float", true
	case typeDouble:
		return "double", true
	case columnTypeString, typeVarchar, "char":
		return "string", true
	case "binary", "varbinary":
		return "bytes", true
	case typeDate:
		return map[string]any{avroKeyType: "int", avroKeyLogical: typeDate}, true
	case typeTimestamp:
		return map[string]any{avroKeyType: "long", avroKeyLogical: "timestamp-millis"}, true
	}

	return nil, false
}

func (t *hiveType) avroSchema(nm *avroNamer) (any, error) {
	if sch, ok := avroPrimitiveSchema(t.base); ok {
		return sch, nil
	}

	switch t.base {
	case hiveDecimal:
		return map[string]any{
			avroKeyType: "bytes", avroKeyLogical: hiveDecimal, "precision": t.prec, "scale": t.scale,
		}, nil
	case hiveArray:
		el, err := t.elem.avroSchema(nm)

		return map[string]any{avroKeyType: hiveArray, "items": nullable(el)}, err
	case hiveMap:
		el, err := t.elem.avroSchema(nm)

		return map[string]any{avroKeyType: hiveMap, "values": nullable(el)}, err
	case hiveStruct:
		return t.structSchema(nm)
	default:
		return nil, fmt.Errorf("%w: %s", errAvroType, t.base)
	}
}

func (t *hiveType) structSchema(nm *avroNamer) (any, error) {
	fields := make([]any, 0, len(t.fields))

	for _, f := range t.fields {
		ft, err := f.typ.avroSchema(nm)
		if err != nil {
			return nil, err
		}

		fields = append(fields, map[string]any{avroKeyName: f.name, avroKeyType: nullable(ft), avroKeyDefault: nil})
	}

	t.name = nm.next("struct_")

	return map[string]any{avroKeyType: "record", avroKeyName: t.name, "fields": fields}, nil
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
		fields = append(fields, map[string]any{avroKeyName: c.name, avroKeyType: nullable(sch), avroKeyDefault: nil})
	}

	b, err := json.Marshal(map[string]any{avroKeyType: "record", avroKeyName: avroRootName, "fields": fields})

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
		return t.arrayFromJSON(v)
	case hiveMap:
		return t.mapFromJSON(v)
	case hiveStruct:
		return t.structFromJSON(v)
	default:
		return t.fromText(jsonScalarText(v))
	}
}

func (t *hiveType) arrayFromJSON(v any) (any, error) {
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
}

func (t *hiveType) mapFromJSON(v any) (any, error) {
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
}

func (t *hiveType) structFromJSON(v any) (any, error) {
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
	case codecDeflate:
		return ocf.Deflate, nil
	case codecSnappy:
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
		return hex.EncodeToString(n[:])
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

type avroOpts struct {
	skip   map[int64]bool
	path   string
	rowIDs bool
}

func (l *tableLoader) decodeAvro(
	ctx context.Context, rd io.Reader, cols []Column, sink *rowSink, opts avroOpts,
) error {
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

	for ord := int64(0); dec.HasNext(); ord++ {
		var obj map[string]any
		if err = dec.Decode(&obj); err != nil {
			return fmt.Errorf("HIVE_CURSOR_ERROR: Failed to read Avro record: %w", err)
		}

		if opts.skip[ord] {
			continue
		}

		obj, _ = avroUnwrap(obj, dec.Schema()).(map[string]any)
		rec, nulls := avroRow(obj, cols, types, opts, ord)

		if err = l.insertJSON(ctx, sink, rec, nulls); err != nil {
			return err
		}
	}

	if err = dec.Error(); err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: %w", err)
	}

	return nil
}

// avroRow maps a decoded record onto the table columns (case-insensitive names).
func avroRow(obj map[string]any, cols []Column, types []*hiveType, opts avroOpts, ord int64) ([]string, []bool) {
	lower := make(map[string]any, len(obj))
	for k, v := range obj {
		lower[strings.ToLower(k)] = v
	}

	extra := 0
	if opts.rowIDs {
		extra = argsTwo
	}

	rec := make([]string, len(cols)+extra)
	nulls := make([]bool, len(cols)+extra)

	for i, c := range cols {
		v, ok := lower[strings.ToLower(c.Name)]
		if nulls[i] = !ok || v == nil; !nulls[i] {
			rec[i] = avroCell(v, types[i])
		}
	}

	if opts.rowIDs {
		rec[len(cols)], rec[len(cols)+1] = opts.path, strconv.FormatInt(ord, 10)
	}

	return rec, nulls
}
