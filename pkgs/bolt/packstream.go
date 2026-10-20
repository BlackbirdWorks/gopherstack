// Package bolt is a server-side implementation of the Bolt protocol (v4.4 and v5.0 to v5.4) and PackStream.
package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// PackStream markers.
const (
	mTinyString = 0x80
	mTinyList   = 0x90
	mTinyMap    = 0xA0
	mTinyStruct = 0xB0
	mNull       = 0xC0
	mFloat64    = 0xC1
	mFalse      = 0xC2
	mTrue       = 0xC3
	mInt8       = 0xC8
	mInt16      = 0xC9
	mInt32      = 0xCA
	mInt64      = 0xCB
	mBytes8     = 0xCC
	mBytes16    = 0xCD
	mBytes32    = 0xCE
	mString8    = 0xD0
	mString16   = 0xD1
	mString32   = 0xD2
	mList8      = 0xD4
	mList16     = 0xD5
	mList32     = 0xD6
	mMap8       = 0xD8
	mMap16      = 0xD9
	mMap32      = 0xDA
	mStruct8    = 0xDC
	mStruct16   = 0xDD
)

const (
	nibble        = 0x0F
	tinyIntEnd    = 0x80
	negTinyStart  = 0xF0
	byteBits      = 8
	width8        = 1
	width16       = 2
	width32       = 4
	width64       = 8
	maxDepth      = 32
	tinyMax       = 16
	maxStructSize = 15
)

// Errors returned while decoding.
var (
	ErrTruncated   = errors.New("bolt: truncated packstream value")
	ErrTooDeep     = errors.New("bolt: packstream nesting too deep")
	ErrBadMarker   = errors.New("bolt: unknown packstream marker")
	ErrUnsupported = errors.New("bolt: unsupported value type")
	ErrTooLarge    = errors.New("bolt: packstream size exceeds message")
)

// Struct is a generic PackStream structure (tag + fields).
type Struct struct {
	Fields []any
	Tag    byte
}

// Decoder reads PackStream values from a bounded in-memory message.
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder returns a decoder over msg.
func NewDecoder(msg []byte) *Decoder { return &Decoder{buf: msg} }

// Remaining reports the undecoded byte count.
func (d *Decoder) Remaining() int { return len(d.buf) - d.pos }

func (d *Decoder) take(n int) ([]byte, error) {
	if n < 0 || n > d.Remaining() {
		return nil, ErrTruncated
	}

	b := d.buf[d.pos : d.pos+n]
	d.pos += n

	return b, nil
}

func (d *Decoder) uintN(n int) (int, error) {
	b, err := d.take(n)
	if err != nil {
		return 0, err
	}

	var v uint64

	for _, x := range b {
		v = v<<byteBits | uint64(x)
	}

	if v > math.MaxInt32 {
		return 0, ErrTooLarge
	}

	return int(v), nil
}

// Decode reads one value.
func (d *Decoder) Decode() (any, error) { return d.decode(0) }

func (d *Decoder) decode(depth int) (any, error) {
	if depth > maxDepth {
		return nil, ErrTooDeep
	}

	b, err := d.take(1)
	if err != nil {
		return nil, err
	}

	m := b[0]

	switch {
	case m < tinyIntEnd:
		return int64(m), nil
	case m >= negTinyStart:
		return int64(int8(m)), nil //nolint:gosec // two's complement tiny int
	case m >= mTinyString && m < mTinyList:
		return d.str(int(m & nibble))
	case m >= mTinyList && m < mTinyMap:
		return d.list(int(m&nibble), depth)
	case m >= mTinyMap && m < mTinyStruct:
		return d.dict(int(m&nibble), depth)
	case m >= mTinyStruct && m < mNull:
		return d.structure(int(m&nibble), depth)
	}

	return d.decodeSized(m, depth)
}

func (d *Decoder) decodeSized(m byte, depth int) (any, error) {
	switch m {
	case mNull:
		return nil, nil //nolint:nilnil // PackStream null
	case mFalse:
		return false, nil
	case mTrue:
		return true, nil
	case mFloat64:
		b, err := d.take(width64)
		if err != nil {
			return nil, err
		}

		return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
	case mInt8, mInt16, mInt32, mInt64:
		return d.integer(m)
	}

	return d.decodeVar(m, depth)
}

func (d *Decoder) integer(m byte) (any, error) {
	n := intWidth(m)

	b, err := d.take(n)
	if err != nil {
		return nil, err
	}

	var v uint64

	for _, x := range b {
		v = v<<byteBits | uint64(x)
	}

	shift := uint(width64-n) * byteBits

	return int64(v<<shift) >> shift, nil //nolint:gosec // sign extension
}

func intWidth(m byte) int {
	switch m {
	case mInt8:
		return width8
	case mInt16:
		return width16
	case mInt32:
		return width32
	default:
		return width64
	}
}

func (d *Decoder) decodeVar(m byte, depth int) (any, error) {
	kind, width := varKind(m)
	if width == 0 {
		return nil, fmt.Errorf("%w: 0x%02X", ErrBadMarker, m)
	}

	n, err := d.uintN(width)
	if err != nil {
		return nil, err
	}

	switch kind {
	case 'b':
		b, terr := d.take(n)
		if terr != nil {
			return nil, terr
		}

		return append([]byte(nil), b...), nil
	case 's':
		return d.str(n)
	case 'l':
		return d.list(n, depth)
	case 'm':
		return d.dict(n, depth)
	default:
		return d.structure(n, depth)
	}
}

func varKind(m byte) (byte, int) {
	switch m {
	case mBytes8, mBytes16, mBytes32:
		return 'b', sizeWidth(m, mBytes8)
	case mString8, mString16, mString32:
		return 's', sizeWidth(m, mString8)
	case mList8, mList16, mList32:
		return 'l', sizeWidth(m, mList8)
	case mMap8, mMap16, mMap32:
		return 'm', sizeWidth(m, mMap8)
	case mStruct8:
		return 't', width8
	case mStruct16:
		return 't', width16
	}

	return 0, 0
}

// sizeWidth maps the 8/16/32-bit variants (consecutive markers starting at first) to a byte width.
func sizeWidth(m, first byte) int {
	switch m - first {
	case 0:
		return width8
	case 1:
		return width16
	default:
		return width32
	}
}

func (d *Decoder) str(n int) (string, error) {
	b, err := d.take(n)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// list and dict never pre-allocate from the peer count beyond what the remaining bytes could hold.
func (d *Decoder) list(n, depth int) ([]any, error) {
	if n > d.Remaining() {
		return nil, ErrTooLarge
	}

	out := make([]any, 0, n)

	for range n {
		v, err := d.decode(depth + 1)
		if err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, nil
}

func (d *Decoder) dict(n, depth int) (map[string]any, error) {
	if n > d.Remaining()/2 {
		return nil, ErrTooLarge
	}

	out := make(map[string]any, n)

	for range n {
		k, err := d.decode(depth + 1)
		if err != nil {
			return nil, err
		}

		ks, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("%w: map key %T", ErrUnsupported, k)
		}

		v, err := d.decode(depth + 1)
		if err != nil {
			return nil, err
		}

		out[ks] = v
	}

	return out, nil
}

func (d *Decoder) structure(n, depth int) (*Struct, error) {
	tag, err := d.take(1)
	if err != nil {
		return nil, err
	}

	fields, err := d.list(n, depth)
	if err != nil {
		return nil, err
	}

	return &Struct{Tag: tag[0], Fields: fields}, nil
}

// Encoder appends PackStream values to a buffer.
type Encoder struct {
	buf     []byte
	version Version
}

// NewEncoder returns an encoder whose graph structures follow the given protocol version.
func NewEncoder(v Version) *Encoder { return &Encoder{version: v} }

// Bytes returns the encoded bytes.
func (e *Encoder) Bytes() []byte { return e.buf }

// Reset clears the buffer.
func (e *Encoder) Reset() { e.buf = e.buf[:0] }

func (e *Encoder) put(b ...byte) { e.buf = append(e.buf, b...) }

func (e *Encoder) putSized(tiny, m8, m16, m32 byte, n int) {
	switch {
	case n < tinyMax && tiny != 0:
		e.put(tiny | byte(n)) //nolint:gosec // n < 16
	case n <= math.MaxUint8 && m8 != 0:
		e.put(m8, byte(n)) //nolint:gosec // n <= 255
	case n <= math.MaxUint16:
		e.buf = binary.BigEndian.AppendUint16(append(e.buf, m16), uint16(n)) //nolint:gosec // n <= 65535
	default:
		e.buf = append(e.buf, m32)
		e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(n)) //nolint:gosec // bounded by message size
	}
}

// Encode appends v. Supported: nil, bool, ints, floats, string, []byte, []any, []string, map[string]any,
// *Struct, Node, Relationship, Path and the values wrapped by Neptune's adapter.
func (e *Encoder) Encode(v any) error { return e.encode(v, 0) }

func (e *Encoder) encode(v any, depth int) error {
	if depth > maxDepth {
		return ErrTooDeep
	}

	if e.encodeScalar(v) {
		return nil
	}

	switch t := v.(type) {
	case []byte:
		e.putSized(0, mBytes8, mBytes16, mBytes32, len(t))
		e.put(t...)
	case []int64:
		return e.encodeList(len(t), depth, func(i int) any { return t[i] })
	case []string:
		return e.encodeList(len(t), depth, func(i int) any { return t[i] })
	case []any:
		return e.encodeList(len(t), depth, func(i int) any { return t[i] })
	case map[string]any:
		return e.encodeMap(t, depth)
	case *Struct:
		return e.encodeStruct(t.Tag, t.Fields, depth)
	case Node:
		return e.encodeStruct(tagNode, e.nodeFields(t), depth)
	case Relationship:
		return e.encodeStruct(tagRel, e.relFields(t), depth)
	case UnboundRelationship:
		return e.encodeStruct(tagUnboundRel, e.unboundFields(t), depth)
	case Path:
		return e.encodeStruct(tagPath, []any{pathNodes(t), pathRels(t, e), t.Indices}, depth)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupported, v)
	}

	return nil
}

func (e *Encoder) encodeScalar(v any) bool {
	switch t := v.(type) {
	case nil:
		e.put(mNull)
	case bool:
		e.encodeBool(t)
	case int:
		e.encodeInt(int64(t))
	case int32:
		e.encodeInt(int64(t))
	case int64:
		e.encodeInt(t)
	case float64:
		e.buf = append(e.buf, mFloat64)
		e.buf = binary.BigEndian.AppendUint64(e.buf, math.Float64bits(t))
	case string:
		e.putSized(mTinyString, mString8, mString16, mString32, len(t))
		e.buf = append(e.buf, t...)
	default:
		return false
	}

	return true
}

func (e *Encoder) encodeBool(b bool) {
	if b {
		e.put(mTrue)
	} else {
		e.put(mFalse)
	}
}

func (e *Encoder) encodeInt(i int64) {
	switch {
	case i >= -16 && i <= 127:
		e.put(byte(i)) //nolint:gosec // tiny int
	case i >= math.MinInt8 && i <= math.MaxInt8:
		e.put(mInt8, byte(i)) //nolint:gosec // range checked
	case i >= math.MinInt16 && i <= math.MaxInt16:
		e.buf = binary.BigEndian.AppendUint16(append(e.buf, mInt16), uint16(i)) //nolint:gosec // range checked
	case i >= math.MinInt32 && i <= math.MaxInt32:
		e.buf = append(e.buf, mInt32)
		e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(i)) //nolint:gosec // range checked
	default:
		e.buf = append(e.buf, mInt64)
		e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(i)) //nolint:gosec // two's complement
	}
}

func (e *Encoder) encodeList(n, depth int, at func(int) any) error {
	e.putSized(mTinyList, mList8, mList16, mList32, n)

	for i := range n {
		if err := e.encode(at(i), depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeMap(m map[string]any, depth int) error {
	e.putSized(mTinyMap, mMap8, mMap16, mMap32, len(m))

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		if err := e.encode(k, depth+1); err != nil {
			return err
		}

		if err := e.encode(m[k], depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeStruct(tag byte, fields []any, depth int) error {
	if len(fields) > maxStructSize {
		return fmt.Errorf("%w: structure with %d fields", ErrUnsupported, len(fields))
	}

	e.put(mTinyStruct|byte(len(fields)), tag) //nolint:gosec // <= 15

	for _, f := range fields {
		if err := e.encode(f, depth+1); err != nil {
			return err
		}
	}

	return nil
}
