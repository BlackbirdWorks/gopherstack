//nolint:mnd,gosec // AMQP field indexes and wire widths are spec constants; lengths are bounded by frame size
package amqp1

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Unmarshal decodes one value from b and returns it with the unread rest.
func Unmarshal(b []byte) (any, []byte, error) {
	d := decoder{b: b}

	v, err := d.value(0)
	if err != nil {
		return nil, nil, err
	}

	return v, d.b, nil
}

type decoder struct{ b []byte }

func (d *decoder) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.b) {
		return nil, ErrShortBuffer
	}

	out := d.b[:n]
	d.b = d.b[n:]

	return out, nil
}

func (d *decoder) byte() (byte, error) {
	s, err := d.take(1)
	if err != nil {
		return 0, err
	}

	return s[0], nil
}

func (d *decoder) u32() (uint32, error) {
	s, err := d.take(4)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(s), nil
}

func (d *decoder) value(depth int) (any, error) {
	if depth > maxDepth {
		return nil, ErrTooDeep
	}

	code, err := d.byte()
	if err != nil {
		return nil, err
	}

	return d.valueOf(code, depth)
}

func (d *decoder) valueOf(code byte, depth int) (any, error) {
	switch {
	case code == codeDescribed:
		return d.described(depth)
	case isFixedCode(code):
		return d.fixed(code)
	case isVariableCode(code):
		return d.variable(code)
	case code == codeList8 || code == codeList32:
		return d.list(code, depth)
	case code == codeMap8 || code == codeMap32:
		return d.mapValue(code, depth)
	case code == codeArray8 || code == codeArray32:
		return d.array(code, depth)
	}

	return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownType, code)
}

func isFixedCode(code byte) bool {
	return code >= codeNull && code <= codeList0 || fixedWidth(code) > 0
}

func isVariableCode(code byte) bool {
	switch code {
	case codeBin8, codeStr8, codeSym8, codeBin32, codeStr32, codeSym32:
		return true
	}

	return false
}

func (d *decoder) described(depth int) (any, error) {
	desc, err := d.value(depth + 1)
	if err != nil {
		return nil, err
	}

	val, err := d.value(depth + 1)
	if err != nil {
		return nil, err
	}

	return Described{Descriptor: desc, Value: val}, nil
}

func (d *decoder) fixed(code byte) (any, error) {
	switch code {
	case codeNull:
		return nil, nil //nolint:nilnil // AMQP null
	case codeTrue:
		return true, nil
	case codeFalse:
		return false, nil
	case codeUint0:
		return uint32(0), nil
	case codeUlong0:
		return uint64(0), nil
	case codeList0:
		return List{}, nil
	}

	return d.fixedData(code)
}

func fixedWidth(code byte) int {
	switch code {
	case codeUbyte, codeByte, codeBoolean, codeSmallUint, codeSmallUlong, codeSmallInt, codeSmallLong:
		return 1
	case codeUshort, codeShort:
		return 2
	case codeUint, codeInt, codeFloat, codeChar:
		return 4
	case codeUlong, codeLong, codeDouble, codeTimestamp:
		return 8
	case codeUUID:
		return 16
	}

	return -1
}

func (d *decoder) fixedData(code byte) (any, error) {
	w := fixedWidth(code)
	if w < 0 {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownType, code)
	}

	s, err := d.take(w)
	if err != nil {
		return nil, err
	}

	return fixedFromBytes(code, s), nil
}

func fixedFromBytes(code byte, s []byte) any {
	switch code {
	case codeUbyte, codeSmallUint:
		if code == codeSmallUint {
			return uint32(s[0])
		}

		return s[0]
	case codeSmallUlong:
		return uint64(s[0])
	case codeBoolean:
		return s[0] != 0
	case codeByte:
		return int8(s[0])
	case codeSmallInt:
		return int32(int8(s[0]))
	case codeSmallLong:
		return int64(int8(s[0]))
	case codeUUID:
		var u UUID
		copy(u[:], s)

		return u
	}

	return wideFromBytes(code, s)
}

func wideFromBytes(code byte, s []byte) any {
	switch code {
	case codeUshort:
		return binary.BigEndian.Uint16(s)
	case codeShort:
		return int16(binary.BigEndian.Uint16(s))
	case codeUint:
		return binary.BigEndian.Uint32(s)
	case codeInt:
		return int32(binary.BigEndian.Uint32(s))
	case codeFloat:
		return math.Float32frombits(binary.BigEndian.Uint32(s))
	case codeChar:
		return rune(binary.BigEndian.Uint32(s))
	case codeUlong:
		return binary.BigEndian.Uint64(s)
	case codeLong:
		return int64(binary.BigEndian.Uint64(s))
	case codeDouble:
		return math.Float64frombits(binary.BigEndian.Uint64(s))
	}

	return time.UnixMilli(int64(binary.BigEndian.Uint64(s))).UTC()
}

func (d *decoder) size(wide bool) (int, error) {
	if !wide {
		n, err := d.byte()

		return int(n), err
	}

	n, err := d.u32()
	if err != nil {
		return 0, err
	}

	if n > math.MaxInt32 {
		return 0, ErrBadValue
	}

	return int(n), nil
}

func (d *decoder) variable(code byte) (any, error) {
	n, err := d.size(code >= codeBin32)
	if err != nil {
		return nil, err
	}

	s, err := d.take(n)
	if err != nil {
		return nil, err
	}

	switch code {
	case codeBin8, codeBin32:
		return append([]byte(nil), s...), nil
	case codeStr8, codeStr32:
		return string(s), nil
	}

	return Symbol(s), nil
}

// compound reads the size/count header of a list, map or array and returns a
// decoder bounded to exactly its payload.
func (d *decoder) compound(code byte) (*decoder, int, error) {
	wide := code == codeList32 || code == codeMap32 || code == codeArray32

	size, err := d.size(wide)
	if err != nil {
		return nil, 0, err
	}

	body, err := d.take(size)
	if err != nil {
		return nil, 0, err
	}

	sub := &decoder{b: body}

	count, err := sub.size(wide)
	if err != nil {
		return nil, 0, err
	}

	return sub, count, nil
}

func (d *decoder) list(code byte, depth int) (any, error) {
	sub, count, err := d.compound(code)
	if err != nil {
		return nil, err
	}

	if count > len(sub.b) {
		return nil, ErrBadValue
	}

	out := make(List, 0, count)

	for range count {
		v, verr := sub.value(depth + 1)
		if verr != nil {
			return nil, verr
		}

		out = append(out, v)
	}

	return out, nil
}

func (d *decoder) mapValue(code byte, depth int) (any, error) {
	sub, count, err := d.compound(code)
	if err != nil {
		return nil, err
	}

	if count%2 != 0 || count > len(sub.b) {
		return nil, ErrBadValue
	}

	out := make(Map, count/2)

	for range count / 2 {
		k, kerr := sub.value(depth + 1)
		if kerr != nil {
			return nil, kerr
		}

		v, verr := sub.value(depth + 1)
		if verr != nil {
			return nil, verr
		}

		if !hashable(k) {
			return nil, ErrBadValue
		}

		out[k] = v
	}

	return out, nil
}

func hashable(k any) bool {
	switch k.(type) {
	case []byte, List, Map, Described:
		return false
	}

	return true
}

func (d *decoder) array(code byte, depth int) (any, error) {
	sub, count, err := d.compound(code)
	if err != nil {
		return nil, err
	}

	elem, err := sub.byte()
	if err != nil {
		return nil, err
	}

	if elem == codeDescribed || elem < 0x50 || count > len(sub.b) {
		return nil, ErrBadValue
	}

	items := make([]any, 0, count)

	for range count {
		v, verr := sub.valueOf(elem, depth+1)
		if verr != nil {
			return nil, verr
		}

		items = append(items, v)
	}

	return typedArray(items), nil
}

// typedArray converts a homogeneous decoded array into a typed Go slice.
func typedArray(items []any) any {
	if len(items) == 0 {
		return []any{}
	}

	switch items[0].(type) {
	case int64:
		return convertArray[int64](items)
	case uint32:
		return convertArray[uint32](items)
	case time.Time:
		return convertArray[time.Time](items)
	case UUID:
		return convertArray[UUID](items)
	case string:
		return convertArray[string](items)
	case Symbol:
		return convertArray[Symbol](items)
	}

	return items
}

func convertArray[T any](items []any) any {
	out := make([]T, len(items))

	for i, it := range items {
		v, ok := it.(T)
		if !ok {
			return items
		}

		out[i] = v
	}

	return out
}
