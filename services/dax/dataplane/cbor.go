// Package dataplane implements enough of the Amazon DAX binary wire protocol
// to serve the core DynamoDB data-plane operations over a raw TCP socket.
//
// The DAX SDK (amazon-dax-go / amazon-dax-client) does not talk HTTP for item
// operations. Instead it opens a socket to the cluster endpoint and exchanges
// CBOR-encoded, method-ID-framed messages. This package speaks that protocol so
// that gopherstack can accept real DAX client connections and delegate the work
// to the existing in-memory DynamoDB backend.
//
// The CBOR codec here is a faithful re-implementation of the encoding used by
// github.com/aws/aws-dax-go/dax/internal/cbor. It is intentionally
// self-contained so the data plane has no third-party runtime dependency.
package dataplane

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/big"
)

// CBOR major-type and size constants (RFC 7049). These mirror the constants the
// DAX client uses on the wire.
const (
	size8  = 0x18
	size16 = 0x19
	size32 = 0x1a
	size64 = 0x1b

	majorPosInt = 0x00
	majorNegInt = 0x20
	majorBytes  = 0x40
	majorUtf    = 0x60
	majorArray  = 0x80
	majorMap    = 0xa0
	majorTag    = 0xc0
	majorSimple = 0xe0

	simpleFalse = majorSimple + 0x14
	simpleTrue  = majorSimple + 0x15
	simpleNil   = majorSimple + 0x16
	simpleF32   = majorSimple + 0x1a
	simpleF64   = majorSimple + 0x1b
	streamBreak = majorSimple + 0x1f

	majorTypeMask = 0xe0
	minorTypeMask = 0x1f
)

// Standard CBOR tags used by the DAX number encoding.
const (
	tagPosBigInt = 2
	tagNegBigInt = 3
	tagDecimal   = 4
)

// DAX-specific tags for DynamoDB set types.
const (
	tagStringSet = 3321
	tagNumberSet = 3322
	tagBinarySet = 3323
)

// Codec sentinel errors.
var (
	// ErrNaN is returned when a value that should be numeric is not.
	ErrNaN = errors.New("cbor: not a number")
	// errUnexpectedMajor indicates a CBOR major type mismatch.
	errUnexpectedMajor = errors.New("cbor: unexpected major type")
	// errObjTooBig guards against oversized string/byte allocations.
	errObjTooBig = errors.New("cbor: object too big")
	// errCannotSkip indicates an unknown type encountered while skipping.
	errCannotSkip = errors.New("cbor: cannot skip unknown type")
)

// maxObjLen guards against absurd allocation sizes from malformed input.
const maxObjLen = 1024 * 1024 * 1024

// uLen converts a non-negative length to uint64. Negative inputs (which cannot
// occur for slice/string lengths) clamp to zero.
func uLen(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}

// clampInt converts a CBOR length value to int, clamping oversized values to
// maxObjLen so callers never allocate unboundedly from malformed input.
func clampInt(v uint64) int {
	if v > maxObjLen {
		return maxObjLen
	}

	return int(v)
}

// Writer emits CBOR matching the DAX wire format.
type Writer struct {
	w   *bufio.Writer
	buf [9]byte
}

// NewWriter wraps w in a buffered CBOR writer.
func NewWriter(w io.Writer) *Writer {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}

	return &Writer{w: bw}
}

// Flush flushes buffered bytes to the underlying writer.
func (w *Writer) Flush() error { return w.w.Flush() }

func (w *Writer) writeType(major byte, value uint64) error {
	switch {
	case value < size8:
		return w.w.WriteByte(major + byte(value))
	case value < 1<<8:
		w.buf[0] = major + size8
		w.buf[1] = byte(value)

		_, err := w.w.Write(w.buf[:2])

		return err
	case value < 1<<16:
		w.buf[0] = major + size16
		binary.BigEndian.PutUint16(w.buf[1:], uint16(value))

		_, err := w.w.Write(w.buf[:3])

		return err
	case value < 1<<32:
		w.buf[0] = major + size32
		binary.BigEndian.PutUint32(w.buf[1:], uint32(value))

		_, err := w.w.Write(w.buf[:5])

		return err
	default:
		w.buf[0] = major + size64
		binary.BigEndian.PutUint64(w.buf[1:], value)

		_, err := w.w.Write(w.buf[:9])

		return err
	}
}

// WriteInt writes a signed integer.
func (w *Writer) WriteInt(v int) error { return w.WriteInt64(int64(v)) }

// WriteInt64 writes a signed 64-bit integer using CBOR's pos/neg int encoding.
func (w *Writer) WriteInt64(v int64) error {
	if v < 0 {
		// ^v lies in [0, math.MaxInt64] for any negative v, so the conversion is
		// always in range.
		return w.writeType(majorNegInt, nonNegU64(^v))
	}

	return w.writeType(majorPosInt, nonNegU64(v))
}

// nonNegU64 converts a known-non-negative int64 to uint64.
func nonNegU64(v int64) uint64 {
	if v < 0 {
		return 0
	}

	return uint64(v)
}

// WriteString writes a UTF-8 text string (major type 3).
func (w *Writer) WriteString(s string) error {
	if err := w.writeType(majorUtf, uLen(len(s))); err != nil {
		return err
	}

	_, err := w.w.WriteString(s)

	return err
}

// WriteBytes writes a byte string (major type 2).
func (w *Writer) WriteBytes(b []byte) error {
	if err := w.writeType(majorBytes, uLen(len(b))); err != nil {
		return err
	}

	_, err := w.w.Write(b)

	return err
}

// WriteRaw writes pre-encoded bytes verbatim (no CBOR header).
func (w *Writer) WriteRaw(b []byte) error {
	_, err := w.w.Write(b)

	return err
}

// WriteByte writes a single raw byte.
func (w *Writer) WriteByte(b byte) error { return w.w.WriteByte(b) }

// WriteArrayHeader writes an array header for n elements.
func (w *Writer) WriteArrayHeader(n int) error { return w.writeType(majorArray, uLen(n)) }

// WriteMapHeader writes a map header for n key/value pairs.
func (w *Writer) WriteMapHeader(n int) error { return w.writeType(majorMap, uLen(n)) }

// WriteMapStreamHeader writes an indefinite-length map header.
func (w *Writer) WriteMapStreamHeader() error { return w.w.WriteByte(majorMap + minorTypeMask) }

// WriteStreamBreak writes the indefinite-length break code.
func (w *Writer) WriteStreamBreak() error { return w.w.WriteByte(streamBreak) }

// WriteNull writes a CBOR null.
func (w *Writer) WriteNull() error { return w.w.WriteByte(simpleNil) }

// WriteBool writes a CBOR boolean.
func (w *Writer) WriteBool(b bool) error {
	if b {
		return w.w.WriteByte(simpleTrue)
	}

	return w.w.WriteByte(simpleFalse)
}

// WriteFloat64 writes an IEEE-754 double.
func (w *Writer) WriteFloat64(v float64) error {
	w.buf[0] = simpleF64
	binary.BigEndian.PutUint64(w.buf[1:], math.Float64bits(v))

	_, err := w.w.Write(w.buf[:9])

	return err
}

// WriteTag writes a CBOR semantic tag header.
func (w *Writer) WriteTag(tag uint64) error { return w.writeType(majorTag, tag) }

// WriteBigInt writes an integer that may exceed int64 range, using big-int tags
// when required. Mirrors the DAX client's bigint encoding.
func (w *Writer) WriteBigInt(v *big.Int) error {
	if v.IsInt64() {
		return w.WriteInt64(v.Int64())
	}

	sign := v.Sign()

	mag := new(big.Int).Set(v)
	if sign < 0 {
		mag.Not(mag) // -1 - v
	}

	tag := byte(tagPosBigInt)
	if sign < 0 {
		tag = tagNegBigInt
	}

	if err := w.WriteTag(uint64(tag)); err != nil {
		return err
	}

	return w.WriteBytes(mag.Bytes())
}

// Reader decodes CBOR from the DAX wire format.
type Reader struct {
	r   *bufio.Reader
	buf [8]byte
}

// NewReader wraps r in a buffered CBOR reader.
func NewReader(r io.Reader) *Reader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}

	return &Reader{r: br}
}

// PeekHeader returns the next header byte without consuming it.
func (r *Reader) PeekHeader() (byte, error) {
	b, err := r.r.Peek(1)
	if err != nil {
		return 0, err
	}

	return b[0], nil
}

func (r *Reader) readHeader() (int, uint64, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	hdr := int(b)

	switch hdr & minorTypeMask {
	default:
		return hdr, uint64(hdr & minorTypeMask), nil
	case size8:
		if _, err = io.ReadFull(r.r, r.buf[:1]); err != nil {
			return 0, 0, err
		}

		return hdr, uint64(r.buf[0]), nil
	case size16:
		if _, err = io.ReadFull(r.r, r.buf[:2]); err != nil {
			return 0, 0, err
		}

		return hdr, uint64(binary.BigEndian.Uint16(r.buf[:2])), nil
	case size32:
		if _, err = io.ReadFull(r.r, r.buf[:4]); err != nil {
			return 0, 0, err
		}

		return hdr, uint64(binary.BigEndian.Uint32(r.buf[:4])), nil
	case size64:
		if _, err = io.ReadFull(r.r, r.buf[:8]); err != nil {
			return 0, 0, err
		}

		return hdr, binary.BigEndian.Uint64(r.buf[:8]), nil
	}
}

func (r *Reader) expect(hdr, major int) error {
	if hdr&majorTypeMask != major {
		return errUnexpectedMajor
	}

	return nil
}

// ReadInt reads a signed integer into an int.
func (r *Reader) ReadInt() (int, error) {
	v, err := r.ReadInt64()

	return int(v), err
}

// ReadInt64 reads a signed 64-bit integer.
func (r *Reader) ReadInt64() (int64, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return 0, err
	}

	switch hdr & majorTypeMask {
	case majorPosInt:
		return signedFromU64(value), nil
	case majorNegInt:
		return ^signedFromU64(value), nil
	default:
		return 0, ErrNaN
	}
}

// signedFromU64 reinterprets a uint64 as int64, preserving the bit pattern. The
// CBOR pos/neg-int decoding relies on wrap-around for values above MaxInt64.
func signedFromU64(v uint64) int64 {
	if v <= math.MaxInt64 {
		return int64(v)
	}

	// v is in (MaxInt64, MaxUint64]. ^v is then in [0, MaxInt64), so the
	// conversion is in range; negate and offset to recover the original value.
	comp := ^v
	if comp > math.MaxInt64 {
		return math.MinInt64
	}

	return -int64(comp) - 1
}

// ReadString reads a UTF-8 text string.
func (r *Reader) ReadString() (string, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return "", err
	}

	if err = r.expect(hdr, majorUtf); err != nil {
		return "", err
	}

	if value == 0 {
		return "", nil
	}

	if value > maxObjLen {
		return "", errObjTooBig
	}

	b := make([]byte, clampInt(value))
	if _, err = io.ReadFull(r.r, b); err != nil {
		return "", err
	}

	return string(b), nil
}

// ReadBytes reads a byte string.
func (r *Reader) ReadBytes() ([]byte, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return nil, err
	}

	if err = r.expect(hdr, majorBytes); err != nil {
		return nil, err
	}

	if value == 0 {
		return []byte{}, nil
	}

	if value > maxObjLen {
		return nil, errObjTooBig
	}

	b := make([]byte, clampInt(value))
	if _, err = io.ReadFull(r.r, b); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadArrayLength reads an array header and returns its element count.
func (r *Reader) ReadArrayLength() (int, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return 0, err
	}

	if err = r.expect(hdr, majorArray); err != nil {
		return 0, err
	}

	return clampInt(value), nil
}

// ReadMapLength reads a map header and returns its pair count.
func (r *Reader) ReadMapLength() (int, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return 0, err
	}

	if err = r.expect(hdr, majorMap); err != nil {
		return 0, err
	}

	return clampInt(value), nil
}

// ReadNull consumes a CBOR null. It returns true if the next value was null.
func (r *Reader) ReadNull() (bool, error) {
	hdr, err := r.PeekHeader()
	if err != nil {
		return false, err
	}

	if hdr != simpleNil {
		return false, nil
	}

	_, err = r.r.ReadByte()

	return true, err
}

// skip reads and discards the next complete CBOR data item.
func (r *Reader) skip() error {
	hdr, value, err := r.readHeader()
	if err != nil {
		return err
	}

	const pairFields = 2

	switch hdr & majorTypeMask {
	case majorPosInt, majorNegInt:
		return nil
	case majorBytes, majorUtf:
		_, err = io.CopyN(io.Discard, r.r, int64(clampInt(value)))

		return err
	case majorArray:
		for range clampInt(value) {
			if err = r.skip(); err != nil {
				return err
			}
		}

		return nil
	case majorMap:
		for range clampInt(value) * pairFields {
			if err = r.skip(); err != nil {
				return err
			}
		}

		return nil
	case majorTag:
		return r.skip()
	case majorSimple:
		return nil
	default:
		return errCannotSkip
	}
}
