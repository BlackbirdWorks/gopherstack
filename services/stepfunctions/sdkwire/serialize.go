package sdkwire

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/document"
)

// ErrBadValue marks a parameter that does not fit its schema.
var ErrBadValue = errors.New("invalid parameter value")

// input serializes Step Functions parameters (PascalCase keys) by schema.
type input struct {
	value  any
	schema *smithy.Schema
	err    error
}

var _ smithy.Serializable = (*input)(nil)

func (in *input) Serialize(s smithy.ShapeSerializer) {
	in.writeStruct(s, in.schema, in.value)
}

func (in *input) failf(format string, args ...any) {
	if in.err == nil {
		in.err = fmt.Errorf("%w: %s", ErrBadValue, fmt.Sprintf(format, args...))
	}
}

func (in *input) writeStruct(s smithy.ShapeSerializer, schema *smithy.Schema, v any) {
	m, ok := v.(map[string]any)
	if !ok {
		in.failf("expected object for %s", schema.ID())

		return
	}

	s.WriteStruct(schema)

	members := schema.Members()
	names := make([]string, 0, len(members))

	for name := range members {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		if val, found := lookupKey(m, name); found && val != nil {
			in.write(s, members[name], val)
		}
	}

	s.CloseStruct()
}

func lookupKey(m map[string]any, name string) (any, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}

	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}

	return nil, false
}

func (in *input) write(s smithy.ShapeSerializer, ms *smithy.Schema, v any) {
	switch ms.Type() {
	case smithy.ShapeTypeStructure:
		in.writeStruct(s, ms, v)
	case smithy.ShapeTypeUnion:
		in.writeUnion(s, ms, v)
	case smithy.ShapeTypeList, smithy.ShapeTypeSet:
		in.writeList(s, ms, v)
	case smithy.ShapeTypeMap:
		in.writeMap(s, ms, v)
	case smithy.ShapeTypeDocument:
		s.WriteDocument(ms, toDocument(v))
	default:
		in.writeScalar(s, ms, v)
	}
}

func (in *input) writeUnion(s smithy.ShapeSerializer, ms *smithy.Schema, v any) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		in.failf("union %s must hold exactly one member", ms.ID())

		return
	}

	for key, val := range m {
		variant := ms.Member(key)
		if variant == nil {
			for name, cand := range ms.Members() {
				if strings.EqualFold(name, key) {
					variant = cand

					break
				}
			}
		}

		if variant == nil {
			in.failf("unknown member %q of union %s", key, ms.ID())

			return
		}

		s.WriteUnion(ms, variant)
		in.write(s, variant, val)
		s.CloseUnion()
	}
}

func (in *input) writeList(s smithy.ShapeSerializer, ms *smithy.Schema, v any) {
	items, ok := v.([]any)
	if !ok {
		in.failf("expected array for %s", ms.ID())

		return
	}

	s.WriteList(ms)

	for _, it := range items {
		in.write(s, ms.ListMember(), it)
	}

	s.CloseList()
}

func (in *input) writeMap(s smithy.ShapeSerializer, ms *smithy.Schema, v any) {
	m, ok := v.(map[string]any)
	if !ok {
		in.failf("expected object for %s", ms.ID())

		return
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	s.WriteMap(ms)

	for _, k := range keys {
		s.WriteKey(ms.MapKey(), k)
		in.write(s, ms.MapValue(), m[k])
	}

	s.CloseMap()
}

func (in *input) writeScalar(s smithy.ShapeSerializer, ms *smithy.Schema, v any) {
	var err error

	switch ms.Type() {
	case smithy.ShapeTypeString, smithy.ShapeTypeEnum:
		var str string
		if str, err = scalarString(v); err == nil {
			s.WriteString(ms, str)
		}
	case smithy.ShapeTypeBoolean:
		b, ok := v.(bool)
		if !ok {
			err = fmt.Errorf("%w: expected boolean", ErrBadValue)
		} else {
			s.WriteBool(ms, b)
		}
	case smithy.ShapeTypeBlob:
		var raw []byte
		if raw, err = blobBytes(v); err == nil {
			s.WriteBlob(ms, raw)
		}
	case smithy.ShapeTypeTimestamp:
		var t time.Time
		if t, err = parseTime(v); err == nil {
			s.WriteTime(ms, t)
		}
	default:
		err = in.writeNumber(s, ms, v)
	}

	if err != nil {
		in.failf("%s: %v", ms.ID(), err)
	}
}

func (in *input) writeNumber(s smithy.ShapeSerializer, ms *smithy.Schema, v any) error {
	f, err := scalarNumber(v)
	if err != nil {
		return err
	}

	switch ms.Type() {
	case smithy.ShapeTypeByte:
		s.WriteInt8(ms, int8(f))
	case smithy.ShapeTypeShort:
		s.WriteInt16(ms, int16(f))
	case smithy.ShapeTypeInteger, smithy.ShapeTypeIntEnum:
		s.WriteInt32(ms, int32(f))
	case smithy.ShapeTypeLong:
		s.WriteInt64(ms, int64(f))
	case smithy.ShapeTypeFloat:
		s.WriteFloat32(ms, float32(f))
	case smithy.ShapeTypeDouble:
		s.WriteFloat64(ms, f)
	case smithy.ShapeTypeBigInteger:
		bi, _ := big.NewFloat(f).Int(nil)
		s.WriteBigInt(ms, bi)
	case smithy.ShapeTypeBigDecimal:
		s.WriteBigFloat(ms, big.NewFloat(f))
	default:
		return fmt.Errorf("%w: unsupported shape type %d", ErrBadValue, ms.Type())
	}

	return nil
}

func scalarString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "", fmt.Errorf("%w: expected string", ErrBadValue)
	}
}

func scalarNumber(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %w", ErrBadValue, err)
		}

		return f, nil
	default:
		return 0, fmt.Errorf("%w: expected number", ErrBadValue)
	}
}

func blobBytes(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%w: expected base64 string", ErrBadValue)
	}

	if raw, err := base64.StdEncoding.DecodeString(s); err == nil {
		return raw, nil
	}

	return []byte(s), nil
}

func parseTime(v any) (time.Time, error) {
	switch t := v.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %w", ErrBadValue, err)
		}

		return parsed, nil
	case float64:
		sec := int64(t)

		return time.Unix(sec, int64((t-float64(sec))*float64(time.Second))).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: timestamp %v", ErrBadValue, v)
	}
}

func toDocument(v any) document.Value {
	switch t := v.(type) {
	case nil:
		return document.Null{}
	case bool:
		return document.Boolean(t)
	case string:
		return document.String(t)
	case float64:
		return document.Number(strconv.FormatFloat(t, 'f', -1, 64))
	case []any:
		out := make(document.List, len(t))
		for i, it := range t {
			out[i] = toDocument(it)
		}

		return out
	case map[string]any:
		out := make(document.Map, len(t))
		for k, it := range t {
			out[k] = toDocument(it)
		}

		return out
	default:
		return document.String(fmt.Sprint(t))
	}
}
