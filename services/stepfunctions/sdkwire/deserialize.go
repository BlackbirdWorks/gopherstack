package sdkwire

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"time"
	"unicode"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/document"
)

// TimeLayout is how timestamps appear in Step Functions task output.
const TimeLayout = "2006-01-02T15:04:05.000Z"

// output collects a response into a JSON-shaped value: PascalCase member
// names, nil members omitted, timestamps as ISO strings, blobs as base64.
type output struct {
	value  any
	schema *smithy.Schema
}

var _ smithy.Deserializable = (*output)(nil)

func (o *output) Deserialize(d smithy.ShapeDeserializer) error {
	v, err := readStruct(d, o.schema)
	o.value = v

	return err
}

func upperFirst(s string) string {
	r := []rune(s)
	if len(r) > 0 {
		r[0] = unicode.ToUpper(r[0])
	}

	return string(r)
}

func readStruct(d smithy.ShapeDeserializer, schema *smithy.Schema) (map[string]any, error) {
	out := map[string]any{}

	err := smithy.ReadStruct(d, schema, func(ms *smithy.Schema) error {
		v, present, err := read(d, ms)
		if err != nil {
			return err
		}

		if present {
			out[upperFirst(ms.MemberName())] = v
		}

		return nil
	})

	return out, err
}

func read(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	switch s.Type() { //nolint:exhaustive // other shape types are not data
	case smithy.ShapeTypeStructure:
		v, err := readStruct(d, s)

		return v, err == nil, err
	case smithy.ShapeTypeUnion:
		return readUnion(d, s)
	case smithy.ShapeTypeList, smithy.ShapeTypeSet:
		return readList(d, s)
	case smithy.ShapeTypeMap:
		return readMap(d, s)
	case smithy.ShapeTypeDocument:
		return readDocument(d, s)
	default:
		return readScalar(d, s)
	}
}

func readUnion(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	var out map[string]any

	err := smithy.ReadUnion(d, s, func(ms *smithy.Schema) error {
		v, present, err := read(d, ms)
		if err != nil {
			return err
		}

		if present {
			out = map[string]any{upperFirst(ms.MemberName()): v}
		}

		return nil
	})

	return out, out != nil && err == nil, err
}

func readList(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	out := []any{}

	err := smithy.ReadList(d, s, func() error {
		v, present, err := read(d, s.ListMember())
		if err != nil {
			return err
		}

		if present {
			out = append(out, v)
		}

		return nil
	})

	return out, err == nil, err
}

func readMap(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	out := map[string]any{}

	err := smithy.ReadMap(d, s, func(k string) error {
		v, present, err := read(d, s.MapValue())
		if err != nil {
			return err
		}

		if present {
			out[k] = v
		}

		return nil
	})

	return out, err == nil, err
}

func readDocument(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	var dv document.Value
	if err := d.ReadDocument(s, &dv); err != nil {
		return nil, false, err
	}

	v, present := fromDocument(dv)

	return v, present, nil
}

func fromDocument(v document.Value) (any, bool) {
	switch t := v.(type) {
	case nil, document.Null:
		return nil, false
	case document.Boolean:
		return bool(t), true
	case document.String:
		return string(t), true
	case document.Number:
		f, _ := strconv.ParseFloat(string(t), 64)

		return f, true
	case document.Blob:
		return base64.StdEncoding.EncodeToString(t), true
	case document.Timestamp:
		return time.Time(t).UTC().Format(TimeLayout), true
	case document.List:
		out := make([]any, 0, len(t))

		for _, it := range t {
			if e, ok := fromDocument(it); ok {
				out = append(out, e)
			}
		}

		return out, true
	case document.Map:
		out := make(map[string]any, len(t))

		for k, it := range t {
			if e, ok := fromDocument(it); ok {
				out[k] = e
			}
		}

		return out, true
	default:
		return nil, false
	}
}

func readScalar(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	switch s.Type() { //nolint:exhaustive // aggregate types are handled by read
	case smithy.ShapeTypeString, smithy.ShapeTypeEnum:
		var v string

		err := d.ReadString(s, &v)

		return v, err == nil, err
	case smithy.ShapeTypeBoolean:
		var v bool

		err := d.ReadBool(s, &v)

		return v, err == nil, err
	case smithy.ShapeTypeBlob:
		var v []byte

		err := d.ReadBlob(s, &v)

		return base64.StdEncoding.EncodeToString(v), err == nil, err
	case smithy.ShapeTypeTimestamp:
		var v time.Time

		err := d.ReadTime(s, &v)

		return v.UTC().Format(TimeLayout), err == nil, err
	default:
		return readNumber(d, s)
	}
}

func readNumber(d smithy.ShapeDeserializer, s *smithy.Schema) (any, bool, error) {
	var (
		f   float64
		err error
	)

	switch s.Type() { //nolint:exhaustive // only numeric shape types reach here
	case smithy.ShapeTypeByte:
		var v int8

		err = d.ReadInt8(s, &v)
		f = float64(v)
	case smithy.ShapeTypeShort:
		var v int16

		err = d.ReadInt16(s, &v)
		f = float64(v)
	case smithy.ShapeTypeInteger, smithy.ShapeTypeIntEnum:
		var v int32

		err = d.ReadInt32(s, &v)
		f = float64(v)
	case smithy.ShapeTypeLong:
		var v int64

		err = d.ReadInt64(s, &v)
		f = float64(v)
	case smithy.ShapeTypeFloat:
		var v float32

		err = d.ReadFloat32(s, &v)
		f = float64(v)
	case smithy.ShapeTypeDouble:
		err = d.ReadFloat64(s, &f)
	case smithy.ShapeTypeBigInteger:
		var v big.Int

		err = d.ReadBigInt(s, &v)
		f, _ = new(big.Float).SetInt(&v).Float64()
	case smithy.ShapeTypeBigDecimal:
		var v big.Float

		err = d.ReadBigFloat(s, &v)
		f, _ = v.Float64()
	default:
		return nil, false, fmt.Errorf("%w: unsupported shape type %d", ErrBadValue, s.Type())
	}

	return f, err == nil, err
}
