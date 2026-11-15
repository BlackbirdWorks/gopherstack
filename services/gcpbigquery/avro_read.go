package gcpbigquery

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	formatAvro       = "AVRO"
	formatORC        = "ORC"
	numericMaxPrec   = 38
	numericMaxScale  = 9
	maxAvroDepth     = 32
	timeLayoutMicros = "15:04:05.999999"
)

// avroField maps an Avro schema to a field per the Avro to BigQuery type mapping of
// https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro (logical types included).
func avroField(name string, s avro.Schema, depth int) (bqsql.Field, error) {
	if depth > maxAvroDepth {
		return bqsql.Field{}, fmt.Errorf("%w: Avro schema is nested too deeply", errLoad)
	}

	f := bqsql.Field{Name: name, Mode: bqsql.ModeRequired}

	s = derefAvro(s)

	if u, ok := s.(*avro.UnionSchema); ok {
		var only avro.Schema

		for _, br := range u.Types() {
			if br.Type() == avro.Null {
				f.Mode = bqsql.ModeNullable

				continue
			}

			if only != nil {
				return f, fmt.Errorf("%w: Avro union with several non-null types is not supported (%s)", errLoad, name)
			}

			only = br
		}

		if only == nil {
			return f, fmt.Errorf("%w: Avro field %s has only the null type", errLoad, name)
		}

		s = derefAvro(only)
	}

	if arr, ok := s.(*avro.ArraySchema); ok {
		inner, err := avroField(name, arr.Items(), depth+1)
		if err != nil {
			return f, err
		}

		inner.Mode = bqsql.ModeRepeated

		return inner, nil
	}

	err := avroFieldType(&f, s, depth)

	return f, err
}

func derefAvro(s avro.Schema) avro.Schema {
	if ref, ok := s.(*avro.RefSchema); ok {
		return ref.Schema()
	}

	return s
}

func avroFieldType(f *bqsql.Field, s avro.Schema, depth int) error {
	switch sc := s.(type) {
	case *avro.RecordSchema:
		f.Type = litRecord

		for _, rf := range sc.Fields() {
			sub, err := avroField(rf.Name(), rf.Type(), depth+1)
			if err != nil {
				return err
			}

			f.Fields = append(f.Fields, sub)
		}
	case *avro.EnumSchema:
		f.Type = bqsql.KindString.RESTName()
	case *avro.FixedSchema:
		f.Type = bqsql.KindBytes.RESTName()

		if sc.Logical() != nil && sc.Logical().Type() == avro.Decimal {
			f.Type = decimalType(sc.Logical())
		}
	case *avro.PrimitiveSchema:
		f.Type = avroPrimitive(sc)
	default:
		return fmt.Errorf("%w: Avro type %s is not supported", errLoad, s.Type())
	}

	return nil
}

func decimalType(l avro.LogicalSchema) string {
	d, ok := l.(*avro.DecimalLogicalSchema)
	if ok && (d.Precision() > numericMaxPrec || d.Scale() > numericMaxScale) {
		return bqsql.KindBigNumeric.RESTName()
	}

	return bqsql.KindNumeric.RESTName()
}

func avroLogicalType(l avro.LogicalSchema) string {
	if l.Type() == avro.Decimal {
		return decimalType(l)
	}

	types := make(map[avro.LogicalType]string)
	types[avro.Date] = bqsql.KindDate.RESTName()

	for _, t := range []avro.LogicalType{avro.TimestampMillis, avro.TimestampMicros, avro.TimestampNanos} {
		types[t] = bqsql.KindTimestamp.RESTName()
	}

	local := []avro.LogicalType{avro.LocalTimestampMillis, avro.LocalTimestampMicros, avro.LocalTimestampNanos}

	for _, t := range local {
		types[t] = bqsql.KindDatetime.RESTName()
	}

	for _, t := range []avro.LogicalType{avro.TimeMillis, avro.TimeMicros} {
		types[t] = bqsql.KindTime.RESTName()
	}

	return types[l.Type()]
}

func avroPrimitive(sc *avro.PrimitiveSchema) string {
	if l := sc.Logical(); l != nil {
		if t := avroLogicalType(l); t != "" {
			return t
		}
	}

	types := make(map[avro.Type]string)
	types[avro.Boolean] = bqsql.KindBool.RESTName()
	types[avro.Int], types[avro.Long] = bqsql.KindInt.RESTName(), bqsql.KindInt.RESTName()
	types[avro.Float], types[avro.Double] = bqsql.KindFloat.RESTName(), bqsql.KindFloat.RESTName()
	types[avro.Bytes] = bqsql.KindBytes.RESTName()

	if t, ok := types[sc.Type()]; ok {
		return t
	}

	return bqsql.KindString.RESTName()
}

func openAvro(data []byte) (*ocf.Decoder, []bqsql.Field, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("avro: %w", err)
	}

	rec, ok := derefAvro(dec.Schema()).(*avro.RecordSchema)
	if !ok {
		return nil, nil, fmt.Errorf("%w: the top-level Avro schema must be a record", errLoad)
	}

	fields := make([]bqsql.Field, 0, len(rec.Fields()))

	for _, f := range rec.Fields() {
		bf, ferr := avroField(f.Name(), f.Type(), 0)
		if ferr != nil {
			return nil, nil, ferr
		}

		fields = append(fields, bf)
	}

	return dec, fields, nil
}

func convertAvroRecord(obj map[string]any, fields []bqsql.Field) map[string]any {
	out := make(map[string]any, len(obj))

	byName := make(map[string]bqsql.Field, len(fields))
	for _, f := range fields {
		byName[f.Name] = f
	}

	for k, v := range obj {
		out[k] = avroCell(v, byName[k], 0)
	}

	return out
}

func detectAvro(data []byte) ([]bqsql.Field, error) {
	_, fields, err := openAvro(data)

	return fields, err
}

func parseAvroInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	dec, file, err := openAvro(data)
	if err != nil {
		return nil, nil, err
	}

	var (
		rows [][]any
		bad  []error
	)

	for n := 1; dec.HasNext(); n++ {
		var obj map[string]any
		if err = dec.Decode(&obj); err != nil {
			return nil, nil, fmt.Errorf("avro: %w", err)
		}

		obj, _ = avroUnwrap(obj, dec.Schema()).(map[string]any)
		obj = convertAvroRecord(obj, file)

		row, perr := bqsql.ParseRecord(fields, obj, bqsql.ParseOptions{IgnoreUnknown: lc.IgnoreExtra})
		if perr != nil {
			bad = append(bad, fmt.Errorf("row %d: %w", n, perr))

			continue
		}

		rows = append(rows, row)
	}

	if err = dec.Error(); err != nil {
		return nil, nil, fmt.Errorf("avro: %w", err)
	}

	return rows, bad, nil
}

// avroCell converts a decoded Avro value to the JSON-like form ParseValue accepts for field f.
func avroCell(v any, f bqsql.Field, depth int) any {
	if v == nil || depth > maxAvroDepth {
		return nil
	}

	if arr, ok := v.([]any); ok && f.Mode == bqsql.ModeRepeated {
		el := f
		el.Mode = bqsql.ModeNullable
		out := make([]any, len(arr))

		for i, e := range arr {
			out[i] = avroCell(e, el, depth+1)
		}

		return out
	}

	if m, ok := v.(map[string]any); ok {
		byName := make(map[string]bqsql.Field, len(f.Fields))
		for _, sub := range f.Fields {
			byName[sub.Name] = sub
		}

		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = avroCell(e, byName[k], depth+1)
		}

		return out
	}

	return avroScalar(v, f.Type)
}

func avroScalar(v any, typ string) any {
	switch t := v.(type) {
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return float64(t)
	case []byte:
		return base64.StdEncoding.EncodeToString(t)
	case time.Time:
		switch typ {
		case bqsql.KindDate.RESTName():
			return t.UTC().Format(time.DateOnly)
		case bqsql.KindDatetime.RESTName():
			return t.UTC().Format("2006-01-02T15:04:05.999999")
		}

		return t.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return time.Unix(0, int64(t)).UTC().Format(timeLayoutMicros)
	case *big.Rat:
		return t.FloatString(numericMaxScale)
	}

	return v
}

func avroUnwrap(v any, s avro.Schema) any {
	switch sc := derefAvro(s).(type) {
	case *avro.UnionSchema:
		return unwrapAvroUnion(v, sc)
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
	}

	return v
}

func avroUnionKey(s avro.Schema) string {
	switch sc := derefAvro(s).(type) {
	case *avro.ArraySchema:
		return string(avro.Array)
	case *avro.MapSchema:
		return string(avro.Map)
	case *avro.RecordSchema:
		return sc.FullName()
	}

	return ""
}

func unwrapAvroUnion(v any, sc *avro.UnionSchema) any {
	if v == nil {
		return nil
	}

	for _, br := range sc.Types() {
		if br.Type() == avro.Null {
			continue
		}

		if m, ok := v.(map[string]any); ok && len(m) == 1 {
			if inner, has := m[avroUnionKey(br)]; has && avroUnionKey(br) != "" {
				return avroUnwrap(inner, br)
			}
		}

		return avroUnwrap(v, br)
	}

	return v
}
