//nolint:mnd,gosec // AMQP field indexes and wire widths are spec constants; lengths are bounded by frame size
package amqp1

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"
)

// Marshal appends the AMQP encoding of v to dst.
//
// Numbers are always written at full width so that peers decoding into an
// untyped value see the exact Go type that was encoded.
func Marshal(dst []byte, v any) ([]byte, error) {
	return appendValue(dst, v)
}

func appendValue(b []byte, v any) ([]byte, error) {
	if out, ok := appendScalar(b, v); ok {
		return out, nil
	}

	switch x := v.(type) {
	case Symbol:
		return appendVar(b, codeSym8, codeSym32, []byte(x)), nil
	case Described:
		return appendDescribed(b, x)
	case List:
		return appendList(b, x)
	case []any:
		return appendList(b, x)
	case Map:
		return appendMap(b, x)
	case map[any]any:
		return appendMap(b, x)
	case map[string]any:
		return appendStringMap(b, x)
	}

	return appendArrayOf(b, v)
}

func appendScalar(b []byte, v any) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return append(b, codeNull), true
	case bool:
		if x {
			return append(b, codeTrue), true
		}

		return append(b, codeFalse), true
	case string:
		return appendVar(b, codeStr8, codeStr32, []byte(x)), true
	case []byte:
		return appendVar(b, codeBin8, codeBin32, x), true
	case time.Time:
		return binary.BigEndian.AppendUint64(append(b, codeTimestamp), uint64(x.UnixMilli())), true
	case UUID:
		return append(append(b, codeUUID), x[:]...), true
	}

	return appendNumber(b, v)
}

func appendNumber(b []byte, v any) ([]byte, bool) {
	switch x := v.(type) {
	case uint8:
		return append(b, codeUbyte, x), true
	case int8:
		return append(b, codeByte, byte(x)), true
	case uint16:
		return binary.BigEndian.AppendUint16(append(b, codeUshort), x), true
	case int16:
		return binary.BigEndian.AppendUint16(append(b, codeShort), uint16(x)), true
	case uint32:
		return binary.BigEndian.AppendUint32(append(b, codeUint), x), true
	case int32:
		return binary.BigEndian.AppendUint32(append(b, codeInt), uint32(x)), true
	case uint64:
		return binary.BigEndian.AppendUint64(append(b, codeUlong), x), true
	case int64:
		return binary.BigEndian.AppendUint64(append(b, codeLong), uint64(x)), true
	case float32:
		return binary.BigEndian.AppendUint32(append(b, codeFloat), math.Float32bits(x)), true
	case float64:
		return binary.BigEndian.AppendUint64(append(b, codeDouble), math.Float64bits(x)), true
	}

	return b, false
}

func appendVar(b []byte, code8, code32 byte, data []byte) []byte {
	if len(data) <= math.MaxUint8 {
		return append(append(b, code8, byte(len(data))), data...)
	}

	b = binary.BigEndian.AppendUint32(append(b, code32), uint32(len(data)))

	return append(b, data...)
}

func appendDescribed(b []byte, d Described) ([]byte, error) {
	b = append(b, codeDescribed)

	if code, ok := d.Descriptor.(uint64); ok && code <= math.MaxUint8 {
		b = append(b, codeSmallUlong, byte(code))
	} else {
		var err error
		if b, err = appendValue(b, d.Descriptor); err != nil {
			return nil, err
		}
	}

	return appendValue(b, d.Value)
}

// appendCompound writes code32, size, count, then the items produced by fn.
func appendCompound(b []byte, code byte, count int, fn func([]byte) ([]byte, error)) ([]byte, error) {
	b = append(b, code, 0, 0, 0, 0, 0, 0, 0, 0)
	start := len(b)

	b, err := fn(b)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(b[start-8:], uint32(len(b)-start+4))
	binary.BigEndian.PutUint32(b[start-4:], uint32(count))

	return b, nil
}

func appendList(b []byte, l []any) ([]byte, error) {
	return appendCompound(b, codeList32, len(l), func(b []byte) ([]byte, error) {
		var err error

		for _, e := range l {
			if b, err = appendValue(b, e); err != nil {
				return nil, err
			}
		}

		return b, nil
	})
}

func appendMap(b []byte, m map[any]any) ([]byte, error) {
	return appendCompound(b, codeMap32, len(m)*2, func(b []byte) ([]byte, error) {
		var err error

		for k, v := range m {
			if b, err = appendValue(b, k); err != nil {
				return nil, err
			}

			if b, err = appendValue(b, v); err != nil {
				return nil, err
			}
		}

		return b, nil
	})
}

func appendStringMap(b []byte, m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return appendCompound(b, codeMap32, len(m)*2, func(b []byte) ([]byte, error) {
		var err error

		for _, k := range keys {
			b = appendVar(b, codeStr8, codeStr32, []byte(k))

			if b, err = appendValue(b, m[k]); err != nil {
				return nil, err
			}
		}

		return b, nil
	})
}

// appendArray writes an AMQP array whose elements share constructor code and
// are produced by put (which writes only the element body).
func appendArray(b []byte, code byte, n int, put func([]byte, int) []byte) []byte {
	b = append(b, codeArray32, 0, 0, 0, 0, 0, 0, 0, 0, code)
	start := len(b) - 1

	for i := range n {
		b = put(b, i)
	}

	binary.BigEndian.PutUint32(b[start-8:], uint32(len(b)-start+4))
	binary.BigEndian.PutUint32(b[start-4:], uint32(n))

	return b
}

func appendArrayOf(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case []int64:
		return appendArray(b, codeLong, len(x), func(b []byte, i int) []byte {
			return binary.BigEndian.AppendUint64(b, uint64(x[i]))
		}), nil
	case []uint32:
		return appendArray(b, codeUint, len(x), func(b []byte, i int) []byte {
			return binary.BigEndian.AppendUint32(b, x[i])
		}), nil
	case []time.Time:
		return appendArray(b, codeTimestamp, len(x), func(b []byte, i int) []byte {
			return binary.BigEndian.AppendUint64(b, uint64(x[i].UnixMilli()))
		}), nil
	case []UUID:
		return appendArray(b, codeUUID, len(x), func(b []byte, i int) []byte {
			return append(b, x[i][:]...)
		}), nil
	case []string:
		return appendArray(b, codeStr32, len(x), func(b []byte, i int) []byte {
			return append(binary.BigEndian.AppendUint32(b, uint32(len(x[i]))), x[i]...)
		}), nil
	case []Symbol:
		return appendArray(b, codeSym32, len(x), func(b []byte, i int) []byte {
			return append(binary.BigEndian.AppendUint32(b, uint32(len(x[i]))), x[i]...)
		}), nil
	}

	return nil, fmt.Errorf("%w: %T", ErrUnsupportedType, v)
}
