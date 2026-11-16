package iceberg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ErrBadBound is returned when a bound or value cannot be serialized for its type.
var ErrBadBound = errors.New("invalid iceberg bound")

// Bound is a decoded column bound: int64, float64, string or bool.
type Bound = any

const (
	sign32 = 1 << 31
	sign64 = 1 << 63
	bytes4 = 4
	bytes8 = 8
)

// EncodeBound serializes a value of the Iceberg primitive type (spec Appendix D, single-value serialization).
// v is int64, float64, string or bool as produced by parsing a result cell.
func EncodeBound(icebergType string, v Bound) ([]byte, error) {
	switch icebergType {
	case typeBoolean:
		return encodeBool(v)
	case typeInt, typeDate:
		return encodeInt32(icebergType, v)
	case typeLong, typeTimestamp, typeTimestamptz, typeTime:
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("%w: %v as %s", ErrBadBound, v, icebergType)
		}

		return binary.LittleEndian.AppendUint64(nil, twosComplement(n)), nil
	case typeFloat, typeDouble:
		return encodeFloat(icebergType, v)
	case typeString:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %v as string", ErrBadBound, v)
		}

		return []byte(s), nil
	default:
		return nil, fmt.Errorf("%w: type %s", ErrBadBound, icebergType)
	}
}

func encodeBool(v Bound) ([]byte, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, fmt.Errorf("%w: %v as boolean", ErrBadBound, v)
	}

	if b {
		return []byte{1}, nil
	}

	return []byte{0}, nil
}

func encodeInt32(icebergType string, v Bound) ([]byte, error) {
	n, ok := v.(int64)
	if !ok || n < math.MinInt32 || n > math.MaxInt32 {
		return nil, fmt.Errorf("%w: %v as %s", ErrBadBound, v, icebergType)
	}

	return binary.LittleEndian.AppendUint32(nil, uint32(n&math.MaxUint32)), nil
}

func encodeFloat(icebergType string, v Bound) ([]byte, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, fmt.Errorf("%w: %v as %s", ErrBadBound, v, icebergType)
	}

	if icebergType == typeFloat {
		return binary.LittleEndian.AppendUint32(nil, math.Float32bits(float32(f))), nil
	}

	return binary.LittleEndian.AppendUint64(nil, math.Float64bits(f)), nil
}

// DecodeBound reverses EncodeBound; types without an ordering the engine uses (decimal, binary, uuid) report ok=false.
func DecodeBound(icebergType string, b []byte) (Bound, bool) {
	switch icebergType {
	case typeBoolean:
		return len(b) == 1 && b[0] != 0, len(b) == 1
	case typeInt, typeDate:
		if len(b) != bytes4 {
			return nil, false
		}

		v := binary.LittleEndian.Uint32(b)

		return int64(v&math.MaxInt32) - int64(v&sign32), true
	case typeLong, typeTimestamp, typeTimestamptz, typeTime:
		if len(b) != bytes8 {
			return nil, false
		}

		return fromTwosComplement(binary.LittleEndian.Uint64(b)), true
	case typeFloat:
		if len(b) != bytes4 {
			return nil, false
		}

		return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), true
	case typeDouble:
		if len(b) != bytes8 {
			return nil, false
		}

		return math.Float64frombits(binary.LittleEndian.Uint64(b)), true
	case typeString:
		return string(b), true
	default:
		return nil, false
	}
}

// CompareBound orders two decoded bounds of the same kind; ok=false when the kinds differ or are unordered.
func CompareBound(a, b Bound) (int, bool) {
	switch x := a.(type) {
	case int64:
		y, ok := b.(int64)

		return cmpOrdered(x, y), ok
	case float64:
		y, ok := b.(float64)

		return cmpOrdered(x, y), ok
	case string:
		y, ok := b.(string)

		return cmpOrdered(x, y), ok
	case bool:
		y, ok := b.(bool)

		return cmpOrdered(strconv.FormatBool(x), strconv.FormatBool(y)), ok
	default:
		return 0, false
	}
}

func cmpOrdered[T int64 | float64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// twosComplement is the bit pattern of n as an unsigned 64-bit integer.
func twosComplement(n int64) uint64 {
	v := uint64(n & math.MaxInt64)
	if n < 0 {
		return v | sign64
	}

	return v
}

func fromTwosComplement(u uint64) int64 {
	v := int64(u & math.MaxInt64)
	if u&sign64 != 0 {
		return v + math.MinInt64
	}

	return v
}
